import os, time, sqlite3, requests

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_DB_ID = os.environ["NOTION_DB_ID"]
NOTION_VERSION = "2022-06-28"
DB_PATH = "hys.db"

def H():
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }

def find_by_url(u: str):
    q = {"filter": {"property": "Link", "url": {"equals": u}}, "page_size": 1}
    r = requests.post(f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query",
                      headers=H(), json=q, timeout=30)
    r.raise_for_status()
    return r.json().get("results", [])

def p_title(x):  return {"title":[{"text":{"content":(x or "(no title)")[:2000]}}]}
def p_select(x): return {"select":{"name":x}} if x else None
def p_url(x):    return {"url":x} if x else None
def p_date(x):   return {"date":{"start":x}} if x else None
def p_rich(x):   return {"rich_text":[{"text":{"content":(x or "")[:1900]}}]} if x else None
def p_multi(xs): return {"multi_select":[{"name":s} for s in xs if s]} if xs else None

def rows():
    con = sqlite3.connect(DB_PATH); con.row_factory = sqlite3.Row
    cur = con.cursor()
    for sql in (
        "SELECT id AS initiative_id, url, title, start_date AS open_from, summary, responsible_service, policy_areas FROM meta",
        "SELECT id AS initiative_id, url, title, start_date AS open_from, summary, responsible_service, policy_areas FROM initiatives",
    ):
        try:
            for r in cur.execute(sql):
                yield r
            return
        except sqlite3.Error:
            continue

def create_page(r: sqlite3.Row):
    dg = [r["responsible_service"]] if r["responsible_service"] else []
    topics = [x.strip() for x in (r["policy_areas"] or "").split(";") if x.strip()]
    props = {
        "Title": p_title(r["title"]),
        "DocType": p_select("Consultation"),
        "Institution": p_select("Commission"),
        "Link": p_url(r["url"]),
        "Date": p_date(r["open_from"]) if r["open_from"] else None,
        "CELEX": p_rich(str(r["initiative_id"])),
        "Summary": p_rich(r["summary"]),
        "DG / Committee": p_multi(dg) if dg else None,
        "Topics": p_multi(topics) if topics else None,
    }
    props = {k:v for k,v in props.items() if v is not None}
    payload = {"parent":{"database_id":NOTION_DB_ID},"properties":props}
    rr = requests.post("https://api.notion.com/v1/pages", headers=H(), json=payload, timeout=30)
    rr.raise_for_status()

def main():
    for r in rows():
        url = r["url"]
        if not url: 
            continue
        if find_by_url(url):
            continue
        create_page(r)
        time.sleep(0.25)

if __name__ == "__main__":
    main()
