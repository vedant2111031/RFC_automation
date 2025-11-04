# crawler_script.py
import asyncio
import csv
import json
import os
import re
import time
from urllib.parse import urljoin, urlparse, urlunparse

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import pandas as pd
from collections import defaultdict, deque

# -------------------------
# Config / Globals
# -------------------------
print("🚀 Python Crawler Script Started — Server is running this file")

START_URL = "https://health-ee.netlify.app/"
DOMAIN = urlparse(START_URL).netloc

# Outputs
OUTPUT_DIR = "exports"
LOCAL_SCREENSHOT_DIR = "screenshots"
PUBLIC_SCREENSHOT_DIR = os.path.join("public", "screenshots")  # served as /screenshots/<file>
OUTPUT_CSV_CTA = os.path.join(OUTPUT_DIR, "cta_tracking_map.csv")
OUTPUT_CSV_FORM = os.path.join(OUTPUT_DIR, "form_tracking_map.csv")
OUTPUT_JSON = os.path.join(OUTPUT_DIR, "cta_form_tracking_map.json")
OUTPUT_EXCEL = os.path.join(OUTPUT_DIR, "cta_sdr_export.xlsx")
OUTPUT_FLOWS = os.path.join(OUTPUT_DIR, "user_flows.xlsx")

# Crawl controls
MAX_PAGES = 10             # absolute pages to crawl
PAGE_NAV_TIMEOUT = 30_000      # ms for page.goto
PAGE_WAIT_AFTER_SCROLL = 1.0   # seconds
HEADLESS = True                # run browser headless or not

# Flow generation limits
MAX_FLOW_DEPTH = 5
MAX_FLOW_COUNT = 10000         # stop generating flows beyond this to avoid explosion

# Status for UI integration (pollable)
crawl_status = {
    "running": False,
    "completed": False,
    "current_url": None,
    "pages_crawled": 0,
    "error": None,
    "total": None
}

# Data stores
site_graph = defaultdict(set)   # page -> set(links)
all_ctas = []
all_forms = []
visited = set()

# Ensure folders exist
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOCAL_SCREENSHOT_DIR, exist_ok=True)
os.makedirs(PUBLIC_SCREENSHOT_DIR, exist_ok=True)

# Regex to ignore mailto/tel/javascript etc.
INVALID_SCHEMES = re.compile(r'^(mailto:|tel:|javascript:|#)', re.I)


# -------------------------
# Helpers
# -------------------------
def normalize_url(base, link):
    """
    Normalize a found link: join, remove fragment, strip trailing slash (except root).
    Returns None if link isn't valid or not http/https or not same domain.
    """
    if not link:
        return None
    link = link.strip()
    # ignore invalid schemes
    if INVALID_SCHEMES.match(link):
        return None

    # Make absolute
    try:
        absolute = urljoin(base, link)
    except Exception:
        return None

    parsed = urlparse(absolute)

    # Only http(s)
    if parsed.scheme not in ("http", "https"):
        return None

    # Same domain only
    # Accept same domain and subdomains (www or others)
    if not (parsed.netloc == DOMAIN or parsed.netloc.endswith("." + DOMAIN)):
        return None



    # Remove fragment
    parsed = parsed._replace(fragment="")

    # Normalise query: optional -> keep queries but could remove in future
    path = parsed.path or "/"
    # remove duplicate slashes
    path = re.sub(r'\/\/+', '/', path)

    # strip trailing slash for non-root
    if path != "/" and path.endswith("/"):
        path = path[:-1]

    parsed = parsed._replace(path=path)
    normalized = urlunparse(parsed)
    return normalized


def safe_filename(s: str) -> str:
    """Make a simple safe filename from URL"""
    return re.sub(r'[^A-Za-z0-9\-_\.]', '_', s)[:200]


# -------------------------
# Extractors
# -------------------------
async def accept_cookies(page):
    """Try common cookie acceptors, tolerant to errors."""
    try:
        # OneTrust
        els = page.locator("#onetrust-accept-btn-handler")
        if await els.count() > 0:
            try:
                await els.first.click()
                await page.wait_for_timeout(800)
                return
            except Exception:
                pass

        # CookieBot / other "Accept All" buttons
        btns = page.locator("button:has-text('Accept All'), button:has-text('Accept Cookies'), button:has-text('Accept')")
        if await btns.count() > 0:
            try:
                await btns.first.click()
                await page.wait_for_timeout(800)
                return
            except Exception:
                pass
    except Exception:
        # swallow any cookie click errors
        pass


async def extract_ctas(page, current_url):
    script = """
    () => {
        return Array.from(
            document.querySelectorAll("a,button,[role='button'],input[type='submit'],input[type='button']")
        ).map(el => {
            const rect = el.getBoundingClientRect();
            return {
                tag: el.tagName.toLowerCase(),
                text: (el.innerText || el.textContent || "").trim(),
                href: el.getAttribute("href") || "",
                id: el.id || "",
                class: el.className || "",
                x: rect.x || 0, y: rect.y || 0, width: rect.width || 0, height: rect.height || 0
            };
        });
    }
    """
    try:
        elements = await page.evaluate(script)
    except Exception:
        elements = []

    ctas = []
    for i, el in enumerate(elements):
        local_screenshot = ""
        public_screenshot = ""
        try:
            # only screenshot visible-ish elements
            if (el.get("width", 0) or 0) > 6 and (el.get("height", 0) or 0) > 6:
                filename = f"cta_{len(all_ctas) + i}_{safe_filename(current_url)}.png"
                local_screenshot = os.path.join(LOCAL_SCREENSHOT_DIR, filename)
                public_screenshot = os.path.join(PUBLIC_SCREENSHOT_DIR, filename)

                # clip values must be ints and non-negative
                clip = {
                    "x": max(int(el.get("x", 0) or 0), 0),
                    "y": max(int(el.get("y", 0) or 0), 0),
                    "width": max(int(el.get("width", 10) or 10), 10),
                    "height": max(int(el.get("height", 10) or 10), 10)
                }
                try:
                    # take screenshots into both locations (overwrite is fine)
                    await page.screenshot(path=local_screenshot, clip=clip)
                    await page.screenshot(path=public_screenshot, clip=clip)
                except Exception:
                    # if screenshot fails, clear names
                    local_screenshot = ""
                    public_screenshot = ""
        except Exception:
            local_screenshot = ""
            public_screenshot = ""

        ctas.append({
            "page_url": current_url,
            "page_name": urlparse(current_url).path.strip("/") or "home",
            "element_type": el.get("tag", ""),
            "text": el.get("text", ""),
            "id_or_class": el.get("id") or el.get("class") or "",
            "link": el.get("href") or "",
            # expose web path (served by Flask at /static or by static hosting at /screenshots)
            "screenshot_local": local_screenshot,
            "screenshot_url": f"/screenshots/{os.path.basename(public_screenshot)}" if public_screenshot else ""
        })
    return ctas


async def extract_forms(page, current_url):
    script = """
    () => {
        return Array.from(document.querySelectorAll("form")).map(form => {
            const rect = form.getBoundingClientRect();
            return {
                form_id: form.id || "",
                form_class: form.className || "",
                form_name: form.getAttribute("name") || "",
                method: form.getAttribute("method") || "GET",
                action: form.getAttribute("action") || "",
                x: rect.x || 0, y: rect.y || 0, width: rect.width || 0, height: rect.height || 0,
                inputs: Array.from(form.querySelectorAll("input, select, textarea")).map(inp => ({
                    type: inp.type || "text",
                    name: inp.name || "",
                    placeholder: inp.placeholder || "",
                    id: inp.id || "",
                    class: inp.className || ""
                })),
                submit_buttons: Array.from(form.querySelectorAll("button[type='submit'], input[type='submit']")).map(btn => ({
                    text: btn.innerText || btn.value || "",
                    id: btn.id || "",
                    class: btn.className || ""
                }))
            };
        });
    }
    """
    try:
        forms = await page.evaluate(script)
    except Exception:
        forms = []

    form_data = []
    for i, form in enumerate(forms):
        local_screenshot = ""
        public_screenshot = ""
        try:
            if (form.get("width", 0) or 0) > 50 and (form.get("height", 0) or 0) > 50:
                filename = f"form_{len(all_forms) + i}_{safe_filename(current_url)}.png"
                local_screenshot = os.path.join(LOCAL_SCREENSHOT_DIR, filename)
                public_screenshot = os.path.join(PUBLIC_SCREENSHOT_DIR, filename)

                clip = {
                    "x": max(int(form.get("x", 0) or 0), 0),
                    "y": max(int(form.get("y", 0) or 0), 0),
                    "width": max(int(form.get("width", 50) or 50), 50),
                    "height": max(int(form.get("height", 50) or 50), 50)
                }
                try:
                    await page.screenshot(path=local_screenshot, clip=clip)
                    await page.screenshot(path=public_screenshot, clip=clip)
                except Exception:
                    local_screenshot = ""
                    public_screenshot = ""
        except Exception:
            local_screenshot = ""
            public_screenshot = ""

        form_data.append({
            "page_url": current_url,
            "page_name": urlparse(current_url).path.strip("/") or "home",
            "form_id_or_class": form.get("form_id") or form.get("form_class") or "",
            "method": form.get("method", "GET"),
            "action": form.get("action") or "",
            "inputs": json.dumps(form.get("inputs", []), ensure_ascii=False),
            "submit_buttons": json.dumps(form.get("submit_buttons", []), ensure_ascii=False),
            "form_screenshot_local": local_screenshot,
            "form_screenshot_url": f"/screenshots/{os.path.basename(public_screenshot)}" if public_screenshot else ""
        })
    return form_data


# -------------------------
# Crawl (iterative BFS-like queue, but you can treat as DFS by using stack)
# -------------------------
async def crawl_site(start_url, max_pages=MAX_PAGES, headless=HEADLESS):
    """
    Main crawling function. Uses a queue to avoid recursion and updates global data stores.
    """
    if not start_url.startswith("http"):
        start_url = "https://" + start_url

    # reset global stores
    site_graph.clear()
    all_ctas.clear()
    all_forms.clear()
    visited.clear()

    crawl_status["running"] = True
    crawl_status["completed"] = False
    crawl_status["current_url"] = None
    crawl_status["pages_crawled"] = 0
    crawl_status["error"] = None
    crawl_status["total"] = max_pages

    # queue entries: (url, depth)
    queue = deque()
    # Normalize and allow non-www and www versions dynamically
    start_norm = normalize_url(start_url, start_url)

    if not start_norm:
    # Instead of stopping the entire crawl, log a warning and continue
        print(f"⚠ Warning: Could not normalize the start URL: {start_url}")
        start_norm = start_url  # Use the raw URL as fallback

# Dynamically set domain if not already correct
    parsed_start = urlparse(start_norm)
    global DOMAIN
    DOMAIN = parsed_start.netloc.replace("www.", "")

    queue.append((start_norm, 0))

    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=headless)
            page = await browser.new_page()

            while queue and len(visited) < max_pages:
                url, depth = queue.popleft()
                if url in visited:
                    continue
                visited.add(url)

                # ✅ Update UI crawl status BEFORE loading page
                crawl_status["current_url"] = url
                crawl_status["pages_crawled"] = len(visited)
                # allow UI polling to pick it up
                await asyncio.sleep(0)

                print(f"[{len(visited)}/{max_pages}] Crawling: {url} (depth={depth})")

                try:
                    # navigate with timeout and wait for DOM
                    try:
                        await page.goto(url, wait_until="domcontentloaded", timeout=PAGE_NAV_TIMEOUT)
                    except PlaywrightTimeoutError:
                        print(f"⚠ Timeout loading {url} (continuing)")
                        # still try to continue extracting minimal content
                    except Exception as e:
                        print(f"⚠ Error navigating to {url}: {e}")

                    # try cookie accept
                    await accept_cookies(page)

                    # scroll to bottom to allow lazy load
                    try:
                        await page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
                        await asyncio.sleep(PAGE_WAIT_AFTER_SCROLL)
                    except Exception:
                        pass

                    # extract CTAs & forms on this page
                    try:
                        ctas = await extract_ctas(page, url)
                        forms = await extract_forms(page, url)
                        all_ctas.extend(ctas)
                        all_forms.extend(forms)
                    except Exception as e:
                        print(f"⚠ Error extracting on {url}: {e}")

                    # update status after extracting
                    crawl_status["pages_crawled"] = len(visited)
                    await asyncio.sleep(0)

                    # find links and normalize
                    links = []
                    try:
                        raw_links = await page.eval_on_selector_all("a[href]", "els => els.map(a => a.getAttribute('href'))")
                        for raw in raw_links:
                            full = normalize_url(url, raw)
                            if full and full not in visited:
                                links.append(full)
                    except Exception:
                        links = []

                    # add to graph and queue (avoid duplicates)
                    for link in links:
                        site_graph[url].add(link)
                        if link not in visited and len(visited) + len(queue) < max_pages:
                            queue.append((link, depth + 1))

                except Exception as e:
                    print(f"⚠ Unexpected error on {url}: {e}")

            await browser.close()

    except Exception as e:
        crawl_status["error"] = str(e)
        print(f"⚠ Fatal error while crawling: {e}")

    # mark completed
    crawl_status["running"] = False
    crawl_status["completed"] = True
    crawl_status["current_url"] = None
    crawl_status["pages_crawled"] = len(visited)
    print("✅ Crawling finished. Pages crawled:", len(visited))


# -------------------------
# Flow generation (DFS-limited)
# -------------------------
def generate_flows(start_url, max_depth=MAX_FLOW_DEPTH, max_flows=MAX_FLOW_COUNT):
    all_flows = []
    start = normalize_url(start_url, start_url)
    if not start:
        return []

    stack = [(start, [start])]
    seen_count = 0

    while stack and len(all_flows) < max_flows:
        current, path = stack.pop()
        # If no outgoing or reached depth limit -> save path
        if len(path) - 1 >= max_depth or not site_graph.get(current):
            all_flows.append(path)
            continue

        # Expand neighbors
        neighbors = list(site_graph.get(current, []))
        # Sort neighbors for determinism (optional)
        neighbors.sort()
        for n in neighbors:
            if n in path:
                # avoid cycles
                continue
            new_path = path + [n]
            stack.append((n, new_path))

        seen_count += 1
        if seen_count > (max_flows * 10):
            # guard against infinite expansion
            break

    return all_flows


# -------------------------
# Exporters
# -------------------------
def export_csvs_and_json():
    # CTA CSV
    with open(OUTPUT_CSV_CTA, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "page_url", "page_name", "element_type", "text",
            "id_or_class", "link", "screenshot_local", "screenshot_url"
        ])
        writer.writeheader()
        writer.writerows([item for item in all_ctas if "element_type" in item])

    # Forms CSV
    with open(OUTPUT_CSV_FORM, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "page_url", "page_name", "form_id_or_class", "method",
            "action", "inputs", "submit_buttons", "form_screenshot_local", "form_screenshot_url"
        ])
        writer.writeheader()
        writer.writerows(all_forms)

    # Combined JSON
    try:
        with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
            json.dump({"ctas": all_ctas, "forms": all_forms}, f, indent=4, ensure_ascii=False)
    except Exception as e:
        print("⚠ Error writing JSON:", e)

    print("✅ CSV & JSON exports done.")


def export_sdr_excel():
    sdr_rows = []
    for item in all_ctas:
        sdr_rows.append({
            "Page URL": item.get("page_url", ""),
            "Page Name": item.get("page_name", ""),
            "CTA Text": item.get("text", ""),
            "Element Type": item.get("element_type", ""),
            "ID / Class": item.get("id_or_class", ""),
            "Destination Link": item.get("link", ""),
            "Screenshot (Web URL)": item.get("screenshot_url", ""),
            "Tracking Variable (eVar/prop)": "",
            "Event (eventX)": "",
            "Data Layer Trigger?": "",
            "Notes": ""
        })
    df = pd.DataFrame(sdr_rows)
    try:
        # use openpyxl engine to avoid xlsxwriter hyperlink warnings
        df.to_excel(OUTPUT_EXCEL, index=False, engine="openpyxl")
        print(f"✅ SDR Excel saved: {OUTPUT_EXCEL}")
    except Exception as e:
        print("⚠ Error writing SDR Excel:", e)


def export_flows_excel(flows):
    # write flows as plain text (joined with arrow) to avoid Excel hyperlink detection
    text_flows = [{"User Flow": " → ".join(flow)} for flow in flows]
    df = pd.DataFrame(text_flows)
    try:
        df.to_excel(OUTPUT_FLOWS, index=False, engine="openpyxl")
        print(f"✅ User flows saved: {OUTPUT_FLOWS} (count={len(flows)})")
    except Exception as e:
        print("⚠ Error writing flows Excel:", e)


# ✅ Updated main() — supports live UI status + keeps your logic
async def main(start_url=START_URL,
               crawl_status_param=None,
               max_pages=MAX_PAGES,
               headless=HEADLESS,
               max_flow_depth=MAX_FLOW_DEPTH,
               max_flows=MAX_FLOW_COUNT):

    print("▶ Starting crawl:", start_url)
    t0 = time.time()

    # Initialize UI live status
    if crawl_status_param is not None:
        crawl_status_param["current_url"] = start_url
        crawl_status_param["pages_crawled"] = 0
        crawl_status_param["total"] = max_pages
        # also point the module-level crawl_status to same dict so crawl_site updates it
        globals()['crawl_status'] = crawl_status_param

    # ✅ Correct: directly await the crawl function
    await crawl_site(start_url, max_pages=max_pages, headless=headless)

    t1 = time.time()
    print(f"▶ Crawl finished in {t1 - t0:.1f}s. Pages crawled: {len(visited)}")

    # ✅ Mark in status that crawl is done
    crawl_status["completed"] = True
    crawl_status["running"] = False

    # exports
    export_csvs_and_json()
    export_sdr_excel()

    # generate flows (may be large) — capped
    flows = generate_flows(start_url, max_depth=max_flow_depth, max_flows=max_flows)
    export_flows_excel(flows)
    # ✅ Zip all screenshots for user download
    zip_public_screenshots()


    print("✅ All exports complete.")
    return {
        "pages_crawled": len(visited),
        "ctas_found": len(all_ctas),
        "forms_found": len(all_forms),
        "flows_generated": len(flows)
    }


# -------------------------
import shutil

def zip_public_screenshots():
    zip_path = os.path.join("public", "screenshots")  # output → public/screenshots.zip

    # Create a zip only if screenshots exist
    if os.path.exists(PUBLIC_SCREENSHOT_DIR) and os.listdir(PUBLIC_SCREENSHOT_DIR):
        shutil.make_archive(zip_path, 'zip', PUBLIC_SCREENSHOT_DIR)
        print("✅ Public screenshots zipped successfully:", zip_path + ".zip")
    else:
        print("⚠ No screenshots found to zip.")

# CLI runner
# -------------------------
if __name__ == "__main__":
    # run standalone
    try:
        result = asyncio.run(main())
        print("Result summary:", result)
    except Exception as exc:
        print("Fatal error:", exc)
        crawl_status["error"] = str(exc)
        crawl_status["running"] = False
        crawl_status["completed"] = True
