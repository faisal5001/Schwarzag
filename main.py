import time
import csv
import os
import signal
import sys
import string
from playwright.sync_api import sync_playwright

# ================= CONFIG =================
BASE_SEARCH = "https://search.ch/tel/?all={query}&page={page}"
OUTPUT_FOLDER = "data"
ALL_FILE = "ALL_COMPANIES_ALL.csv"          # combined all records
UNIQUE_COMBINED_FILE = "ALL_COMPANIES_UNIQUE.csv"  # combined unique records
HEADERS = ["query", "company_name", "company_link", "phone", "website", "address", "categories"]
MAX_PAGES = 5000
RETRY_LIMIT = 3
RESUME_FILE = "last_page.txt"

os.makedirs(OUTPUT_FOLDER, exist_ok=True)
RUNNING = True
total_saved_unique = 0
total_saved_all = 0

# ================= LOGGER =================
def log(msg):
    print(f"[DEBUG] {msg}")

# ================= CTRL+C SAFE STOP =================
def stop_scraper(sig, frame):
    global RUNNING
    RUNNING = False
    log(" CTRL+C detected! Stopping safely...")

signal.signal(signal.SIGINT, stop_scraper)

# ================= GLOBAL DEDUP =================
seen_global_unique = set()  # for combined unique CSV

# ================= CSV SAVE =================
def append_all_csv(item):
    """Save all records combined"""
    global total_saved_all
    path = os.path.join(OUTPUT_FOLDER, ALL_FILE)
    file_exists = os.path.isfile(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        if not file_exists:
            writer.writeheader()
        writer.writerow(item)
        f.flush()
    total_saved_all += 1

def append_unique_combined(item):
    """Save to combined unique CSV across all parts"""
    global seen_global_unique
    if not item.get("company_link"):
        return
    key = item["company_link"].lower()
    if key in seen_global_unique:
        return
    seen_global_unique.add(key)
    path = os.path.join(OUTPUT_FOLDER, UNIQUE_COMBINED_FILE)
    file_exists = os.path.isfile(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        if not file_exists:
            writer.writeheader()
        writer.writerow(item)

# ================= LAST PAGE RESUME =================
def save_last_page(query, page_num):
    with open(RESUME_FILE, "w") as f:
        f.write(f"{query}|{page_num}")

def load_last_page(query):
    if not os.path.isfile(RESUME_FILE):
        return 1
    with open(RESUME_FILE, "r") as f:
        data = f.read().strip()
        if data.startswith(query + "|"):
            return int(data.split("|")[1])
    return 1

# ================= SCRAPER =================
def scrape_query(page, base_query, suffixes=None):
    if suffixes is None:
        suffixes = [""]

    for suf in suffixes:
        query = f"{base_query} {suf}".strip()
        filename = f"{query.replace(' ', '_')}.csv"  # unique CSV per keyword/suffix
        log(f"\n SEARCHING KEYWORD: {query}")

        page_num = load_last_page(query)

        # TEMP storage for suffix-level unique records
        seen_suffix_unique = set()
        suffix_unique_items = []

        while RUNNING and page_num <= MAX_PAGES:
            url = BASE_SEARCH.format(query=query, page=page_num)
            for attempt in range(RETRY_LIMIT):
                try:
                    log(f"⚡ Page {page_num} -> {url}")
                    page.goto(url, timeout=60000)
                    page.wait_for_timeout(3000)
                    break
                except Exception as e:
                    log(f" Load error {attempt + 1}: {e}")
                    time.sleep(2)
            else:
                break

            # ================= LAZY LOADING SCROLL =================
            last_height = 0
            for _ in range(8):
                page.mouse.wheel(0, 4000)
                page.wait_for_timeout(2500)
                new_height = page.evaluate("document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
            # ======================================================

            rows = page.query_selector_all("ol li article")
            log(f"ROWS FOUND = {len(rows)}")
            if not rows:
                log(f" No results for {query}, stopping")
                break

            for r in rows:
                if not RUNNING:
                    break
                try:
                    name = r.query_selector("h1 a")
                    phone = r.query_selector("div.tel-number a")
                    web = r.query_selector("a.sl-icon-website")
                    addr = r.query_selector("div.tel-address")
                    cat = r.query_selector("div.tel-categories")
                    item = {
                        "query": query,
                        "company_name": name.inner_text().strip() if name else "",
                        "company_link": "https://search.ch" + name.get_attribute("href") if name else "",
                        "phone": phone.inner_text().strip() if phone else "",
                        "website": web.get_attribute("href") if web else "",
                        "address": addr.inner_text().strip() if addr else "",
                        "categories": cat.inner_text().strip() if cat else "",
                    }
                    print("DEBUG ITEM:", item["company_name"], item["phone"])

                    # Save all combined (includes duplicates)
                    append_all_csv(item)

                    # Collect suffix-level unique records
                    key = item["company_link"].lower() if item.get("company_link") else None
                    if key and key not in seen_suffix_unique:
                        seen_suffix_unique.add(key)
                        suffix_unique_items.append(item)

                    # Update global combined unique CSV
                    append_unique_combined(item)

                except Exception as e:
                    log(f"Parse error: {e}")

            page_num += 1
            save_last_page(query, page_num)

        # ================= SAVE UNIQUE CSV FOR THIS SUFFIX =================
        if suffix_unique_items:
            path = os.path.join(OUTPUT_FOLDER, filename)
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=HEADERS)
                writer.writeheader()
                writer.writerows(suffix_unique_items)
            log(f"✔ UNIQUE CSV SAVED FOR: {filename} ({len(suffix_unique_items)} records)")

# ================= MAIN =================
def main():
    if len(sys.argv) < 2:
        print("Usage: python main.py taxi | taxi a | all")
        return

    presets = ["restaurant", "hotel", "doctor", "taxi"]
    auto_suffixes = list(string.ascii_lowercase) + list(string.digits)

    first_arg = sys.argv[1].lower()
    suffix_args = sys.argv[2:]

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.route("**/*", lambda route, request:
                   route.abort() if request.resource_type in ["image", "stylesheet", "font"]
                   else route.continue_())

        # ================= CASE 1: ALL MODE =================
        if first_arg == "all":
            for category in presets:
                print(f"\n===== BASE: {category} =====")
                scrape_query(page, category)
                print(f"\n===== SUFFIX MODE: {category} a-z 0-9 =====")
                scrape_query(page, category, suffixes=auto_suffixes)

        # ================= CASE 2: SINGLE CATEGORY (with or without suffix) =================
        else:
            category = first_arg
            if suffix_args:
                suffix_str = " ".join(suffix_args)
                scrape_query(page, category, suffixes=[suffix_str])
            else:
                scrape_query(page, category)

        browser.close()

    log(f" FINISHED. TOTAL RECORDS SAVED (ALL) = {total_saved_all}")
    log(f" FINISHED. UNIQUE RECORDS ACROSS ALL PARTS = {len(seen_global_unique)}")

# ================= RUN =================
if __name__ == "__main__":
    log(" SCRAPER STARTED")
    main()
