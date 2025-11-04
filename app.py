from flask import Flask, render_template, request, send_from_directory, jsonify
import threading
import asyncio
from crawler_script import main as crawl_website  # Ensure this accepts (url, crawl_status)

app = Flask(
    __name__, 
    template_folder="templates", 
    static_folder="public"
)

# Global crawl state
crawl_status = {
    "running": False,
    "completed": False,
    "current_url": "",
    "pages_crawled": 0,
    "total": 0
}

# ✅ Background Crawler Runner
def run_crawler(url):
    crawl_status["running"] = True
    crawl_status["completed"] = False
    crawl_status["current_url"] = url
    crawl_status["pages_crawled"] = 0

    asyncio.run(crawl_website(url, crawl_status))  # Pass crawl status to main()
    
    crawl_status["running"] = False
    crawl_status["completed"] = True

# ✅ Home Page
@app.route('/')
def home():
    return render_template('index.html')

# ✅ Start Crawl
@app.route('/start', methods=['POST'])
def start_crawl():
    url = request.form.get("url")
    thread = threading.Thread(target=run_crawler, args=(url,))
    print("🚀 Script started")
    thread.start()
    return render_template("loading.html", url=url)

# ✅ Status API
@app.route('/status')
def status():
    return jsonify(crawl_status)

# ✅ Results Page
@app.route('/results')
def results():
    return render_template("results.html")

# ✅ Download Exported Files
@app.route('/download/<path:filename>')
def download(filename):
    return send_from_directory("exports", filename, as_attachment=True)

# ✅ Download Screenshots ZIP (ensure you save it in /public or update path)
@app.route('/download/screenshots.zip')
def download_screenshots():
    return send_from_directory('public', 'screenshots.zip', as_attachment=True)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
