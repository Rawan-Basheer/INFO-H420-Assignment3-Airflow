from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import json
import sqlite3
import requests
import fitz # PyMuPDF
from openai import OpenAI

# Paths Setup
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GDRIVE_DIR = os.path.join(BASE_DIR, "Google Drive", "ResearchLibrary")
PAPERS_DIR = os.path.join(GDRIVE_DIR, "papers")
OUTPUT_DIR = os.path.join(GDRIVE_DIR, "output")
DB_PATH = os.path.join(BASE_DIR, "data", "papers.db")
HTML_PATH = os.path.join(OUTPUT_DIR, "index.html")

DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/YOUR_WEBHOOK_HERE"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 11, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS papers (
            filename TEXT PRIMARY KEY,
            title TEXT,
            summary TEXT,
            keywords TEXT,
            category TEXT
        )
    """)
    conn.commit()
    conn.close()

def pdf_to_markdown(pdf_path):
    doc = fitz.open(pdf_path)
    text = ""
    for page in doc:
        text += page.get_text()
    return text

def extract_metadata_llm(text):
    client = OpenAI(base_url="http://localhost:11434/v1", api_key="ollama")
    prompt = f"""
    Analyze the following research paper text and return a JSON object with strictly these keys:
    "title", "summary", "keywords" (array of strings), "category".

    Text snippet:
    {text[:4000]}
    """
    for attempt in range(2):
        try:
            response = client.chat.completions.create(
                model="llama3",
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"}
            )
            data = json.loads(response.choices[0].message.content)
            if all(k in data for k in ("title", "summary", "keywords", "category")):
                return data
        except Exception as e:
            if attempt == 1:
                raise e
    return {
        "title": "Unknown Title",
        "summary": "Extraction failed.",
        "keywords": ["Uncategorized"],
        "category": "General"
    }

def process_pdfs(**context):
    init_db()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    processed_count = 0
    
    if not os.path.exists(PAPERS_DIR):
        os.makedirs(PAPERS_DIR, exist_ok=True)

    for file in os.listdir(PAPERS_DIR):
        if file.endswith(".pdf"):
            cursor.execute("SELECT filename FROM papers WHERE filename = ?", (file,))
            if cursor.fetchone() is None:
                pdf_path = os.path.join(PAPERS_DIR, file)
                md_text = pdf_to_markdown(pdf_path)
                meta = extract_metadata_llm(md_text)
                cursor.execute(
                    "INSERT INTO papers VALUES (?, ?, ?, ?, ?)",
                    (file, meta['title'], meta['summary'], json.dumps(meta['keywords']), meta['category'])
                )
                conn.commit()
                processed_count += 1

    conn.close()
    return processed_count

def build_static_html(**context):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT filename, title, summary, keywords, category FROM papers")
    rows = cursor.fetchall()
    conn.close()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    html_content = """<!DOCTYPE html>
<html>
<head>
    <title>Research Library</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; background: #f4f6f9; }
        .card { background: white; padding: 20px; margin-bottom: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .tag { background: #e2e8f0; padding: 4px 8px; border-radius: 4px; font-size: 12px; margin-right: 5px; }
        input { padding: 10px; width: 300px; margin-bottom: 20px; }
    </style>
</head>
<body>
    <h1>Research Library Dashboard</h1>
    <input type="text" id="searchInput" onkeyup="filterPapers()" placeholder="Search title or keywords...">
    <div id="papersList">"""
    
    for r in rows:
        kw_list = json.loads(r[3])
        tags = "".join([f'<span class="tag">{k}</span>' for k in kw_list])
        html_content += f"""
        <div class="card">
            <h3>{r[1]} <small>({r[0]})</small></h3>
            <p><strong>Category:</strong> {r[4]}</p>
            <p>{r[2]}</p>
            <div>{tags}</div>
        </div>"""
        
    html_content += """
    </div>
    <script>
        function filterPapers() {
            let input = document.getElementById('searchInput').value.toLowerCase();
            let cards = document.getElementsByClassName('card');
            for (let card of cards) {
                card.style.display = card.innerText.toLowerCase().includes(input) ? "" : "none";
            }
        }
    </script>
</body>
</html>"""
    
    with open(HTML_PATH, "w", encoding="utf-8") as f:
        f.write(html_content)

def send_discord(**context):
    count = context['ti'].xcom_pull(task_ids='process_pdfs_task')
    if count and count > 0:
        msg = {"content": f" **Airflow Pipeline Update**: {count} new paper(s) processed and updated in the library!"}
        requests.post(DISCORD_WEBHOOK_URL, json=msg)

with DAG(
    'paper_processing_pipeline',
    default_args=default_args,
    description='Automated Research PDF Pipeline with LLM & SQLite',
    schedule_interval='@hourly',
    catchup=False
) as dag:

    t1 = PythonOperator(task_id='process_pdfs_task', python_callable=process_pdfs)
    t2 = PythonOperator(task_id='build_static_html_task', python_callable=build_static_html)
    t3 = PythonOperator(task_id='send_discord_notification_task', python_callable=send_discord)

    t1 >> t2 >> t3

