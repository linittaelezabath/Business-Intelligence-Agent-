"""
Skylark Drones — Business Intelligence Agent
Streamlit App (fully self-contained, no external AI APIs)
"""

import os
import re
import math
import requests
import pandas as pd
import streamlit as st
from datetime import datetime, date
from dateutil import parser as dateparser
from collections import defaultdict

# ─────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Skylark BI Agent",
    page_icon="🚁",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────────────────
# CUSTOM CSS
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

    .main-header {
        background: linear-gradient(135deg, #0f172a 0%, #1e293b 50%, #0f4c75 100%);
        color: white;
        padding: 2rem 2.5rem;
        border-radius: 16px;
        margin-bottom: 1.5rem;
        box-shadow: 0 8px 32px rgba(15,76,117,0.3);
    }
    .main-header h1 { font-size: 2rem; font-weight: 700; margin: 0; letter-spacing: -0.5px; }
    .main-header p  { color: #94a3b8; margin: 0.4rem 0 0; font-size: 0.95rem; }

    .metric-card {
        background: #1e293b;
        border: 1px solid #334155;
        border-radius: 12px;
        padding: 1.2rem 1.5rem;
        text-align: center;
        color: white;
    }
    .metric-card .val { font-size: 1.6rem; font-weight: 700; color: #38bdf8; }
    .metric-card .lbl { font-size: 0.78rem; color: #94a3b8; margin-top: 0.2rem; text-transform: uppercase; letter-spacing: 0.05em; }

    .chat-bubble-user {
        background: #1e40af;
        color: white;
        padding: 0.8rem 1.1rem;
        border-radius: 12px 12px 4px 12px;
        margin: 0.4rem 0;
        max-width: 80%;
        margin-left: auto;
        font-size: 0.93rem;
    }
    .chat-bubble-agent {
        background: #1e293b;
        border: 1px solid #334155;
        color: #e2e8f0;
        padding: 1rem 1.2rem;
        border-radius: 12px 12px 12px 4px;
        margin: 0.4rem 0;
        max-width: 92%;
        font-size: 0.93rem;
    }
    .intent-badge {
        display: inline-block;
        background: #0f4c75;
        color: #7dd3fc;
        font-size: 0.7rem;
        font-family: 'JetBrains Mono', monospace;
        padding: 0.15rem 0.5rem;
        border-radius: 999px;
        margin-bottom: 0.5rem;
        text-transform: uppercase;
        letter-spacing: 0.08em;
    }
    .conn-success { color: #4ade80; font-weight: 600; }
    .conn-error   { color: #f87171; font-weight: 600; }

    .stButton > button {
        background: #1d4ed8;
        color: white;
        border: none;
        border-radius: 8px;
        padding: 0.55rem 1.5rem;
        font-weight: 600;
        font-size: 0.9rem;
        transition: background 0.2s;
        width: 100%;
    }
    .stButton > button:hover { background: #2563eb; }

    .stTextInput > div > div > input {
        background: #1e293b;
        border: 1px solid #334155;
        color: white;
        border-radius: 8px;
    }
    .sidebar-section {
        background: #1e293b;
        border: 1px solid #334155;
        border-radius: 10px;
        padding: 1rem;
        margin-bottom: 1rem;
    }
    .sidebar-section h4 { color: #94a3b8; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.1em; margin: 0 0 0.6rem; }

    [data-testid="stSidebar"] { background: #0f172a; }
    .stApp { background: #0a0f1e; }

    div[data-testid="metric-container"] {
        background: #1e293b;
        border: 1px solid #334155;
        border-radius: 10px;
        padding: 0.8rem;
    }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# MONDAY.COM API LAYER
# ─────────────────────────────────────────────────────────────────────────────

MONDAY_URL = "https://api.monday.com/v2"

def monday_query(token: str, query: str, variables: dict = None) -> dict:
    headers = {
        "Content-Type": "application/json",
        "Authorization": token,
        "API-Version": "2024-01",
    }
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    r = requests.post(MONDAY_URL, headers=headers, json=payload, timeout=30)
    r.raise_for_status()
    data = r.json()
    if "errors" in data:
        raise RuntimeError(f"Monday API error: {data['errors']}")
    return data["data"]


def list_boards(token):
    data = monday_query(token, "{ boards(limit: 50) { id name items_count } }")
    return pd.DataFrame(data["boards"])


def get_board_items(token, board_id, limit=500):
    q = """
    query($id: [ID!], $limit: Int) {
      boards(ids: $id) {
        name columns { id title type }
        items_page(limit: $limit) {
          items {
            id name
            column_values { id text value column { title type } }
          }
        }
      }
    }
    """
    data = monday_query(token, q, {"id": [board_id], "limit": limit})
    board = data["boards"][0]
    return board["name"], board["columns"], board["items_page"]["items"]


def items_to_df(items):
    rows = []
    for item in items:
        row = {"_id": item["id"], "_name": item["name"]}
        for cv in item["column_values"]:
            row[cv["column"]["title"]] = cv["text"] or None
        rows.append(row)
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────────────────────────────────────
# DATA CLEANING
# ─────────────────────────────────────────────────────────────────────────────

DATE_FMTS = ["%d/%m/%Y","%m/%d/%Y","%Y-%m-%d","%d-%m-%Y","%d %b %Y","%d %B %Y","%b %d, %Y","%B %d, %Y","%Y/%m/%d","%d.%m.%Y"]

def parse_date(v):
    if pd.isna(v) or str(v).strip() == "": return pd.NaT
    v = str(v).strip()
    for fmt in DATE_FMTS:
        try: return datetime.strptime(v, fmt).date()
        except: pass
    try: return dateparser.parse(v, dayfirst=True).date()
    except: return pd.NaT

def parse_currency(v):
    if pd.isna(v): return float("nan")
    c = re.sub(r"[^\d.]", "", str(v).replace(",", ""))
    try: return float(c) if c else float("nan")
    except: return float("nan")

def norm_text(v):
    if pd.isna(v): return None
    return " ".join(str(v).split()).strip()

def norm_status(v):
    if pd.isna(v): return None
    raw = str(v).strip().lower()
    mapping = {
        "in progress":"In Progress","in-progress":"In Progress","wip":"In Progress",
        "done":"Completed","complete":"Completed","completed":"Completed","closed":"Completed",
        "not started":"Not Started","new":"Not Started","open":"Not Started",
        "stuck":"Blocked","blocked":"Blocked","on hold":"On Hold",
        "won":"Won","closed won":"Won","close won":"Won",
        "lost":"Lost","closed lost":"Lost",
        "qualified":"Qualified","lead":"Lead","prospect":"Prospect",
        "proposal":"Proposal","negotiation":"Negotiation","negotiating":"Negotiation",
    }
    return mapping.get(raw, " ".join(w.capitalize() for w in raw.split()))

def auto_clean(df):
    df = df.copy()
    DATE_KW     = ["date","deadline","due","close","start","end","created"]
    CURRENCY_KW = ["value","amount","revenue","price","budget","cost","worth","arr","mrr"]
    STATUS_KW   = ["status","stage","phase","state","priority"]
    SECTOR_KW   = ["sector","industry","vertical","type","category","segment"]
    for col in df.columns:
        if col.startswith("_"): continue
        cl = col.lower()
        if any(k in cl for k in DATE_KW):
            df[col] = pd.to_datetime(df[col].apply(parse_date), errors="coerce")
        elif any(k in cl for k in CURRENCY_KW):
            df[col] = df[col].apply(parse_currency)
        elif any(k in cl for k in STATUS_KW):
            df[col] = df[col].apply(norm_status)
        else:
            df[col] = df[col].apply(norm_text)
    df["_name"] = df["_name"].apply(norm_text)
    return df

def find_col(df, keywords):
    for kw in keywords:
        for col in df.columns:
            if kw.lower() in col.lower():
                return col
    return None


# ─────────────────────────────────────────────────────────────────────────────
# NLP ENGINE
# ─────────────────────────────────────────────────────────────────────────────

STOPWORDS = {
    "a","an","the","is","are","was","were","be","been","being","have","has","had",
    "do","does","did","will","would","could","should","may","might","shall","can",
    "i","me","my","we","us","our","you","your","it","its","this","that","these",
    "those","what","which","who","how","when","where","why","all","any","each",
    "few","more","most","some","no","not","only","same","so","than","too","very",
    "just","but","if","or","and","of","to","in","for","on","with","at","by",
    "from","up","about","into","through","during","before","after","above","below",
    "give","me","show","tell","get","list","find","whats","their","let","like",
    "looking","overall","across","both",
}

INTENT_VOCAB = {
    "pipeline_overview": {
        "tokens":  ["pipeline","funnel","deals","stages","stage","breakdown","overview","summary","health","status","prospects"],
        "bigrams": ["pipeline_health","sales_pipeline","deal_stage","stage_breakdown"],
        "weight":  1.0,
    },
    "revenue": {
        "tokens":  ["revenue","value","amount","income","sales","booking","arr","mrr","contracted","money","worth","total"],
        "bigrams": ["total_value","deal_value","booking_value","total_revenue"],
        "weight":  1.0,
    },
    "sector_analysis": {
        "tokens":  ["sector","industry","vertical","energy","healthcare","technology","manufacturing","retail","finance","segment"],
        "bigrams": ["sector_performance","by_sector","sector_revenue","industry_breakdown"],
        "weight":  1.2,
    },
    "work_orders": {
        "tokens":  ["work","order","orders","project","projects","operational","completion","complete","completed","active","execution","overdue","blocked","delay","delayed"],
        "bigrams": ["work_order","work_orders","completion_rate","project_status"],
        "weight":  1.0,
    },
    "at_risk": {
        "tokens":  ["risk","stall","stalling","stuck","overdue","late","delayed","attention","warn","warning","flag","problem","issue","concern"],
        "bigrams": ["at_risk","risk_deals","stalled_deals","overdue_deals"],
        "weight":  1.3,
    },
    "forecast": {
        "tokens":  ["forecast","prediction","projection","outlook","expect","future","next","upcoming","trend","trajectory"],
        "bigrams": ["revenue_forecast","pipeline_forecast","next_quarter"],
        "weight":  1.2,
    },
    "leadership_update": {
        "tokens":  ["leadership","update","briefing","report","weekly","board","executive","summary","kpi","metrics","dashboard"],
        "bigrams": ["leadership_update","weekly_update","executive_summary","kpi_dashboard"],
        "weight":  1.2,
    },
}

SECTOR_ENTITIES = [
    "energy","oil","gas","power","utilities","healthcare","pharma","medical","hospital",
    "technology","tech","software","it","manufacturing","industrial","factory",
    "retail","ecommerce","consumer","finance","banking","insurance","fintech",
    "real estate","construction","infrastructure","agriculture","agri","food",
    "telecom","logistics","transport","defence","defense","government",
]
STAGE_ENTITIES = [
    "lead","prospect","qualified","proposal","negotiation","negotiating",
    "won","lost","closed","in progress","completed","blocked","on hold",
]

def tokenise(text):
    text = re.sub(r"[^\w\s]", " ", text.lower())
    return [t for t in text.split() if t not in STOPWORDS and len(t) > 1]

def bigrams(tokens):
    return [f"{tokens[i]}_{tokens[i+1]}" for i in range(len(tokens)-1)]

def classify_intent(query):
    toks = tokenise(query)
    bigs = set(bigrams(toks))
    tset = set(toks)
    scores = {}
    for intent, cfg in INTENT_VOCAB.items():
        score = sum(1.0 for t in cfg["tokens"] if t in tset)
        score += sum(2.0 for b in cfg["bigrams"] if b in bigs)
        scores[intent] = score * cfg["weight"]
    best = max(scores, key=scores.get) if max(scores.values()) > 0 else "general"
    return best, scores, toks

def extract_time(query):
    q = query.lower(); r = {}
    qm = re.search(r"\bq([1-4])\b", q)
    if qm: r["quarter"] = f"Q{qm.group(1)}"
    if re.search(r"\b(this|current)\s+quarter\b", q):
        now = datetime.now()
        r["quarter"] = f"Q{(now.month-1)//3+1}"; r["year"] = now.year; r["relative"] = "current"
    elif re.search(r"\b(last|previous|prior)\s+quarter\b", q):
        now = datetime.now(); pq = (now.month-1)//3
        r["quarter"] = f"Q{pq if pq > 0 else 4}"; r["year"] = now.year if pq > 0 else now.year-1; r["relative"] = "previous"
    months = ["january","february","march","april","may","june","july","august","september","october","november","december","jan","feb","mar","apr","jun","jul","aug","sep","oct","nov","dec"]
    for m in months:
        if re.search(r"\b" + m + r"\b", q): r["month"] = m.capitalize(); break
    ym = re.search(r"\b(20\d{2})\b", query)
    if ym: r["year"] = int(ym.group(1))
    if re.search(r"\b(ytd|year.to.date)\b", q): r["relative"] = "ytd"
    return r

def extract_sector(query):
    q = query.lower()
    for s in SECTOR_ENTITIES:
        if re.search(r"\b" + re.escape(s) + r"\b", q): return s.title()
    return None

def extract_stage(query):
    q = query.lower()
    for s in STAGE_ENTITIES:
        if re.search(r"\b" + re.escape(s) + r"\b", q): return s.title()
    return None

def extract_entities(query):
    return {"time": extract_time(query), "sector": extract_sector(query), "stage": extract_stage(query), "top_n": None}


# ─────────────────────────────────────────────────────────────────────────────
# QUERY ENGINE
# ─────────────────────────────────────────────────────────────────────────────

def _filter_time(df, te, dcol):
    if not te or not dcol or dcol not in df.columns: return df
    now = datetime.now(); mask = pd.Series([True]*len(df), index=df.index)
    dc = pd.to_datetime(df[dcol], errors="coerce")
    if "year"    in te: mask &= dc.dt.year == te["year"]
    if "quarter" in te: mask &= dc.dt.quarter == int(te["quarter"][1])
    if "month"   in te:
        mmap = {"Jan":1,"Feb":2,"Mar":3,"Apr":4,"May":5,"Jun":6,"Jul":7,"Aug":8,"Sep":9,"Oct":10,"Nov":11,"Dec":12,"January":1,"February":2,"March":3,"April":4,"June":6,"July":7,"August":8,"September":9,"October":10,"November":11,"December":12}
        mn = mmap.get(te["month"])
        if mn: mask &= dc.dt.month == mn
    if te.get("relative") == "ytd":
        mask &= (dc.dt.year == now.year) & (dc <= pd.Timestamp(now))
    return df[mask]

def _filter_sector(df, sector, scol):
    if not sector or not scol or scol not in df.columns: return df
    m = df[scol].str.lower().str.contains(sector.lower(), na=False)
    return df[m] if m.sum() > 0 else df

def _ss(s):  return float(s.dropna().sum())
def _sm(s):  vals = s.dropna(); return float(vals.mean()) if len(vals) > 0 else 0.0
def _fmt(v):
    if v >= 1_000_000: return f"₹{v/1_000_000:.2f}M"
    elif v >= 1_000:   return f"₹{v/1_000:.1f}K"
    return f"₹{v:,.0f}"

def q_pipeline(deals, wo, val_col, status_col, sector_col, wo_val_col, wo_status_col, wo_sector_col, entities):
    df = deals.copy()
    dcol = find_col(df, ["date","created","close","due"])
    if entities["time"]:   df = _filter_time(df, entities["time"], dcol)
    if entities["sector"]: df = _filter_sector(df, entities["sector"], sector_col)
    total = len(df); total_val = _ss(df[val_col]) if val_col else 0
    sb = {}
    if status_col and val_col:
        for st, grp in df.groupby(status_col): sb[st] = {"count": len(grp), "value": _ss(grp[val_col])}
    elif status_col:
        for st, grp in df.groupby(status_col): sb[st] = {"count": len(grp), "value": 0}
    won_c  = sum(sb[s]["count"] for s in sb if "won"  in s.lower())
    lost_c = sum(sb[s]["count"] for s in sb if "lost" in s.lower())
    denom  = total - lost_c
    return {"type":"pipeline_overview","total_deals":total,"total_value":total_val,"stage_breakdown":sb,"won_count":won_c,"lost_count":lost_c,"win_rate":(won_c/denom*100 if denom>0 else 0),"filter_sector":entities["sector"],"filter_time":entities["time"],"data_quality":f"{int(df[val_col].isna().sum())} deals missing value" if val_col else "No value col"}

def q_revenue(deals, wo, val_col, status_col, sector_col, wo_val_col, wo_status_col, wo_sector_col, entities):
    df = deals.copy()
    dcol = find_col(df, ["date","created","close"])
    if entities["time"]:   df = _filter_time(df, entities["time"], dcol)
    if entities["sector"]: df = _filter_sector(df, entities["sector"], sector_col)
    if entities.get("stage") and status_col:
        m = df[status_col].str.lower().str.contains(entities["stage"].lower(), na=False)
        if m.sum() > 0: df = df[m]
    tv = _ss(df[val_col]) if val_col else 0
    av = _sm(df[val_col]) if val_col else 0
    mx = None
    if val_col and len(df) > 0 and not df[val_col].isna().all():
        idx = df[val_col].idxmax(); mx = {"name": df.loc[idx,"_name"], "value": df.loc[idx,val_col]}
    by_sec = {}
    if sector_col and val_col: by_sec = df.groupby(sector_col)[val_col].sum().sort_values(ascending=False).head(8).to_dict()
    return {"type":"revenue","count":len(df),"total_value":tv,"avg_value":av,"largest_deal":mx,"by_sector":by_sec,"filter_sector":entities["sector"],"filter_stage":entities.get("stage"),"filter_time":entities["time"],"missing_value":int(df[val_col].isna().sum()) if val_col else "N/A"}

def q_sector(deals, wo, val_col, status_col, sector_col, wo_val_col, wo_status_col, wo_sector_col, entities):
    r = {"type":"sector_analysis","deals":{},"work_orders":{}}
    if sector_col and val_col:
        s = deals.groupby(sector_col)[val_col].agg(["count","sum","mean"])
        s.columns = ["deals","total_value","avg_value"]
        r["deals"] = s.sort_values("total_value", ascending=False).head(10).to_dict("index")
    if wo_sector_col:
        if wo_val_col:
            w = wo.groupby(wo_sector_col)[wo_val_col].agg(["count","sum"]); w.columns = ["orders","total_value"]
        else:
            w = wo.groupby(wo_sector_col)["_id"].count().rename("orders").to_frame(); w["total_value"] = 0
        r["work_orders"] = w.sort_values("orders", ascending=False).head(10).to_dict("index")
    return r

def q_work_orders(deals, wo, val_col, status_col, sector_col, wo_val_col, wo_status_col, wo_sector_col, entities):
    df = wo.copy()
    dcol = find_col(df, ["date","start","due","deadline","end"])
    if entities["time"]:   df = _filter_time(df, entities["time"], dcol)
    if entities["sector"]: df = _filter_sector(df, entities["sector"], wo_sector_col)
    total = len(df); sc = df[wo_status_col].value_counts().to_dict() if wo_status_col else {}
    completed = sum(v for k,v in sc.items() if any(x in str(k).lower() for x in ["complet","done","won"]))
    blocked   = sum(v for k,v in sc.items() if any(x in str(k).lower() for x in ["block","stuck"]))
    in_prog   = sum(v for k,v in sc.items() if any(x in str(k).lower() for x in ["progress","active"]))
    overdue = 0
    if dcol and dcol in df.columns:
        now_ts = pd.Timestamp(datetime.now()); dc_ts = pd.to_datetime(df[dcol], errors="coerce")
        if wo_status_col:
            not_done = ~df[wo_status_col].str.lower().str.contains("complet|done|won", na=False)
            overdue = int(((dc_ts < now_ts) & not_done & dc_ts.notna()).sum())
        else:
            overdue = int(((dc_ts < now_ts) & dc_ts.notna()).sum())
    return {"type":"work_orders","total":total,"completed":completed,"in_progress":in_prog,"blocked":blocked,"overdue":overdue,"completion_rate":(completed/total*100 if total>0 else 0),"total_value":_ss(df[wo_val_col]) if wo_val_col else 0,"status_breakdown":sc,"filter_sector":entities["sector"],"filter_time":entities["time"]}

def q_at_risk(deals, wo, val_col, status_col, sector_col, wo_val_col, wo_status_col, wo_sector_col, entities):
    df = deals.copy(); now_ts = pd.Timestamp(datetime.now()); dcol = find_col(df, ["close","due","deadline","date"]); items = []
    for _, row in df.iterrows():
        reasons = []
        if dcol and pd.notna(row.get(dcol)):
            try:
                cdt = pd.to_datetime(row[dcol])
                if cdt < now_ts: reasons.append(f"Close date overdue by {(now_ts-cdt).days}d")
            except: pass
        if status_col and str(row.get(status_col,"")).lower() in ["blocked","on hold","stuck"]:
            reasons.append(f"Status: {row[status_col]}")
        if reasons:
            items.append({"name":row["_name"],"value":row.get(val_col),"stage":row.get(status_col,"Unknown"),"reasons":reasons})
    items.sort(key=lambda x: float(x["value"] or 0) if x["value"] and not math.isnan(float(x["value"] or 0)) else 0, reverse=True)
    trv = sum(float(x["value"]) for x in items if x["value"] and not math.isnan(float(x["value"] or 0)))
    return {"type":"at_risk","count":len(items),"items":items[:10],"total_at_risk_value":trv}

def q_forecast(deals, wo, val_col, status_col, sector_col, wo_val_col, wo_status_col, wo_sector_col, entities):
    PROB = {"lead":0.10,"prospect":0.20,"qualified":0.35,"proposal":0.50,"negotiation":0.75,"negotiating":0.75,"won":1.0,"closed won":1.0,"lost":0.0,"closed lost":0.0}
    df = deals.copy(); wt = 0.0; by_stage = {}
    if status_col and val_col:
        for _, row in df.iterrows():
            stage = str(row.get(status_col,"")).lower()
            try: value = float(row.get(val_col,0) or 0)
            except: value = 0.0
            prob = PROB.get(stage, 0.30); wv = value * prob; wt += wv
            sd = row.get(status_col,"Unknown")
            if sd not in by_stage: by_stage[sd] = {"count":0,"raw_value":0,"weighted_value":0,"probability":prob}
            by_stage[sd]["count"] += 1; by_stage[sd]["raw_value"] += value; by_stage[sd]["weighted_value"] += wv
    return {"type":"forecast","weighted_forecast":wt,"total_pipeline":_ss(df[val_col]) if val_col else 0,"by_stage":by_stage,"note":"Lead 10% | Prospect 20% | Qualified 35% | Proposal 50% | Negotiation 75% | Won 100%"}

QUERY_DISPATCH = {
    "pipeline_overview": q_pipeline,
    "revenue":           q_revenue,
    "sector_analysis":   q_sector,
    "work_orders":       q_work_orders,
    "at_risk":           q_at_risk,
    "forecast":          q_forecast,
}


# ─────────────────────────────────────────────────────────────────────────────
# RESPONSE FORMATTERS → Return (markdown_text, dataframe_or_None)
# ─────────────────────────────────────────────────────────────────────────────

def _tl(te):
    if not te: return "all time"
    p = []
    if "quarter" in te: p.append(te["quarter"])
    if "month"   in te: p.append(te["month"])
    if "year"    in te: p.append(str(te["year"]))
    if "relative"in te: p.append(te["relative"].replace("_"," "))
    return ", ".join(p) if p else "all time"

def fmt_pipeline(r):
    t = _tl(r["filter_time"]); sec = f" — {r['filter_sector']} sector" if r["filter_sector"] else ""
    md = f"**Total Deals:** {r['total_deals']}  |  **Pipeline Value:** {_fmt(r['total_value'])}  |  **Win Rate:** {r['win_rate']:.1f}%\n\n"
    if r["win_rate"] < 20: md += f"⚠️ Win rate at **{r['win_rate']:.1f}%** — review qualification criteria\n\n"
    elif r["win_rate"] > 50: md += f"🎯 Strong win rate of **{r['win_rate']:.1f}%**\n\n"
    if r["won_count"] > 0: md += f"✅ **{r['won_count']} deals won**\n"
    if r.get("data_quality"): md += f"\n> 📝 _{r['data_quality']}_"
    df_out = None
    if r["stage_breakdown"]:
        rows = [{"Stage": st, "Count": info["count"], "Value": _fmt(info["value"])} for st, info in sorted(r["stage_breakdown"].items(), key=lambda x: x[1]["value"], reverse=True)]
        df_out = pd.DataFrame(rows)
    return md, df_out

def fmt_revenue(r):
    md = f"**Deals:** {r['count']}  |  **Total Value:** {_fmt(r['total_value'])}  |  **Avg Deal:** {_fmt(r['avg_value'])}\n\n"
    if r["largest_deal"]: md += f"🏆 Largest deal: **{r['largest_deal']['name']}** ({_fmt(r['largest_deal']['value'])})\n\n"
    if isinstance(r["missing_value"], int) and r["missing_value"] > 0:
        md += f"> 📝 _{r['missing_value']} deals missing value data_\n"
    df_out = None
    if r["by_sector"]:
        df_out = pd.DataFrame({"Sector": list(r["by_sector"].keys()), "Total Value": [_fmt(v) for v in r["by_sector"].values()]})
    return md, df_out

def fmt_sector(r):
    md = "Cross-board sector breakdown:\n"
    df_out = None
    if r["deals"]:
        rows = [{"Sector": sn, "Deals": int(info["deals"]), "Total Value": _fmt(info["total_value"]), "Avg Value": _fmt(info["avg_value"])} for sn, info in r["deals"].items()]
        df_out = pd.DataFrame(rows)
    return md, df_out

def fmt_work_orders(r):
    md = f"**Total:** {r['total']}  |  **✅ Completed:** {r['completed']} ({r['completion_rate']:.1f}%)  |  **🔄 In Progress:** {r['in_progress']}\n\n"
    if r["blocked"] > 0: md += f"🚫 **{r['blocked']} blocked orders** — needs immediate attention\n"
    if r["overdue"]  > 0: md += f"⏰ **{r['overdue']} overdue orders** — review timelines\n"
    if r["completion_rate"] >= 75: md += "\n🎯 Strong completion rate!"
    elif r["completion_rate"] < 40: md += "\n⚠️ Low completion rate — consider resource reallocation"
    df_out = None
    if r["status_breakdown"]:
        df_out = pd.DataFrame({"Status": list(r["status_breakdown"].keys()), "Count": list(r["status_breakdown"].values())}).sort_values("Count", ascending=False)
    return md, df_out

def fmt_at_risk(r):
    md = f"**{r['count']} at-risk deals** flagged"
    if r["total_at_risk_value"] > 0: md += f"  |  **At-risk value: {_fmt(r['total_at_risk_value'])}**"
    md += "\n\n"
    if not r["items"]: return md + "✅ No at-risk deals identified.", None
    rows = []
    for item in r["items"]:
        vs = _fmt(float(item["value"])) if item["value"] and not math.isnan(float(item["value"] or 0)) else "N/A"
        rows.append({"Deal": item["name"], "Value": vs, "Stage": item["stage"], "Risk": "; ".join(item["reasons"])})
    return md, pd.DataFrame(rows)

def fmt_forecast(r):
    cov = (r["weighted_forecast"]/r["total_pipeline"]*100) if r["total_pipeline"] > 0 else 0
    md = f"**Total Pipeline:** {_fmt(r['total_pipeline'])}  |  **Weighted Forecast:** {_fmt(r['weighted_forecast'])}  |  **Coverage:** {cov:.1f}%\n\n"
    md += f"> 📝 _{r['note']}_\n"
    df_out = None
    if r["by_stage"]:
        rows = [{"Stage": st, "Count": info["count"], "Raw Value": _fmt(info["raw_value"]), "Prob": f"{info['probability']*100:.0f}%", "Weighted": _fmt(info["weighted_value"])} for st, info in sorted(r["by_stage"].items(), key=lambda x: x[1]["weighted_value"], reverse=True)]
        df_out = pd.DataFrame(rows)
    return md, df_out

def fmt_general(r):
    md = f"**Deals:** {r['deals_count']} | **Pipeline Value:** {_fmt(r['deals_value'])}\n\n**Work Orders:** {r['wo_count']} | **WO Value:** {_fmt(r['wo_value'])}\n\nTry asking:\n- *How's our pipeline looking?*\n- *Show me at-risk deals*\n- *Revenue forecast*\n- *Work order completion rate*"
    return md, None

def fmt_leadership(p, w, a, f):
    won_val = sum(p["stage_breakdown"].get(s,{}).get("value",0) for s in p["stage_breakdown"] if "won" in s.lower())
    kpis = {"Pipeline Value": _fmt(p["total_value"]), "Active Deals": str(p["total_deals"]), "Win Rate": f"{p['win_rate']:.1f}%", "Won Value": _fmt(won_val), "Weighted Forecast": _fmt(f["weighted_forecast"]), "WO Completion": f"{w['completion_rate']:.1f}%", "Blocked WOs": str(w["blocked"]), "Overdue WOs": str(w["overdue"])}
    md = f"**Leadership Snapshot — {date.today().strftime('%d %B %Y')}**\n\n"
    md += "**Pipeline stage breakdown (top 5 by value):**\n"
    for st, info in sorted(p["stage_breakdown"].items(), key=lambda x: x[1]["value"], reverse=True)[:5]:
        md += f"- **{st}**: {info['count']} deals | {_fmt(info['value'])}\n"
    md += f"\n**Ops:** ✅ Completed: {w['completed']}  |  🔄 In Progress: {w['in_progress']}  |  🚫 Blocked: {w['blocked']}\n\n"
    if a["count"] > 0:
        md += f"**⚠️ {a['count']} at-risk deals** totalling {_fmt(a['total_at_risk_value'])}\n"
        for item in a["items"][:3]:
            vs = _fmt(float(item["value"])) if item["value"] else "N/A"
            md += f"  - {item['name']} ({vs}): {', '.join(item['reasons'])}\n"
    else:
        md += "✅ No critical risks identified\n"
    return md, pd.DataFrame({"KPI": list(kpis.keys()), "Value": list(kpis.values())})

RENDER_DISPATCH = {
    "pipeline_overview": fmt_pipeline,
    "revenue":           fmt_revenue,
    "sector_analysis":   fmt_sector,
    "work_orders":       fmt_work_orders,
    "at_risk":           fmt_at_risk,
    "forecast":          fmt_forecast,
}

INTENT_LABELS = {
    "pipeline_overview": "📊 Pipeline Overview",
    "revenue":           "💰 Revenue Analysis",
    "sector_analysis":   "🏭 Sector Analysis",
    "work_orders":       "⚙️ Work Orders",
    "at_risk":           "⚠️ At-Risk Deals",
    "forecast":          "🔮 Forecast",
    "leadership_update": "🚁 Leadership Update",
    "general":           "📋 General",
}


# ─────────────────────────────────────────────────────────────────────────────
# SESSION STATE INIT
# ─────────────────────────────────────────────────────────────────────────────

for key, default in {
    "connected": False,
    "deals": None,
    "wo": None,
    "val_col": None,
    "status_col": None,
    "sector_col": None,
    "wo_val_col": None,
    "wo_status_col": None,
    "wo_sector_col": None,
    "chat_history": [],
    "context": {},
    "boards_df": None,
}.items():
    if key not in st.session_state:
        st.session_state[key] = default


# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR — CONNECTION PANEL
# ─────────────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("""
    <div style="text-align:center; padding: 1rem 0 0.5rem;">
        <span style="font-size:2.5rem">🚁</span>
        <h2 style="color:white; margin:0; font-size:1.2rem; font-weight:700;">Skylark BI Agent</h2>
        <p style="color:#64748b; font-size:0.75rem; margin:0.2rem 0 0;">Pure NLP · No AI API needed</p>
    </div>
    """, unsafe_allow_html=True)

    st.divider()

    # Token input
    token = st.text_input(
        "Monday.com API Token",
        type="password",
        value=os.getenv("MONDAY_API_TOKEN", ""),
        placeholder="Paste your token here",
        help="Found in Monday.com → Profile → Admin → API"
    )

    # Board keywords
    col1, col2 = st.columns(2)
    with col1:
        deals_kw = st.text_input("Deals board", value="deal", placeholder="keyword")
    with col2:
        wo_kw = st.text_input("Work Orders board", value="work order", placeholder="keyword")

    connect_btn = st.button("🔗 Connect & Load Data", use_container_width=True)

    if connect_btn and token:
        with st.spinner("Connecting to Monday.com..."):
            try:
                boards_df = list_boards(token)
                st.session_state["boards_df"] = boards_df

                # Find boards
                def find_board(df, kw):
                    m = df["name"].str.lower().str.contains(kw.lower())
                    if m.sum() == 0: raise ValueError(f"No board matching '{kw}'")
                    return str(df[m].iloc[0]["id"]), df[m].iloc[0]["name"]

                deals_id, deals_name = find_board(boards_df, deals_kw)
                wo_id, wo_name = find_board(boards_df, wo_kw)

                # Fetch & clean
                _, _, di = get_board_items(token, deals_id)
                deals = auto_clean(items_to_df(di))
                _, _, wi = get_board_items(token, wo_id)
                wo = auto_clean(items_to_df(wi))

                # Detect columns
                vc  = find_col(deals, ["value","amount","revenue","arr","deal size"])
                sc  = find_col(deals, ["stage","status","phase"])
                sec = find_col(deals, ["sector","industry","vertical","segment"])
                wvc = find_col(wo, ["value","amount","budget","revenue","cost","price"])
                wsc = find_col(wo, ["status","stage","state"])
                wec = find_col(wo, ["sector","industry","client","category","type"])

                # Store
                st.session_state.update({
                    "connected": True, "deals": deals, "wo": wo,
                    "val_col": vc, "status_col": sc, "sector_col": sec,
                    "wo_val_col": wvc, "wo_status_col": wsc, "wo_sector_col": wec,
                })
                st.success(f"✅ Connected!\n\n📋 **{deals_name}** — {len(deals)} deals\n📋 **{wo_name}** — {len(wo)} work orders")
            except Exception as e:
                st.error(f"❌ Connection failed:\n{e}")

    if st.session_state["connected"]:
        st.divider()
        st.markdown("**📋 Board Status**")
        st.markdown(f"- Deals: **{len(st.session_state['deals'])}** rows")
        st.markdown(f"- Work Orders: **{len(st.session_state['wo'])}** rows")
        st.markdown(f"- Value col: `{st.session_state['val_col']}`")
        st.markdown(f"- Status col: `{st.session_state['status_col']}`")

    st.divider()

    # Quick queries
    st.markdown("**⚡ Quick Queries**")
    quick = [
        "How's our pipeline looking?",
        "Show me at-risk deals",
        "Revenue forecast",
        "Work order completion rate",
        "Which sectors perform best?",
        "Give me a leadership update",
    ]
    for q in quick:
        if st.button(q, key=f"quick_{q}", use_container_width=True):
            st.session_state["_pending_query"] = q

    st.divider()
    if st.button("🗑️ Clear Chat", use_container_width=True):
        st.session_state["chat_history"] = []
        st.session_state["context"] = {}
        st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# MAIN PANEL
# ─────────────────────────────────────────────────────────────────────────────

st.markdown("""
<div class="main-header">
    <h1>🚁 Skylark Drones — Business Intelligence</h1>
    <p>Natural language queries on Monday.com data · Pure NLP · No AI API</p>
</div>
""", unsafe_allow_html=True)

# ── Top KPI row (if connected) ────────────────────────────────────────────
if st.session_state["connected"]:
    deals   = st.session_state["deals"]
    wo      = st.session_state["wo"]
    vc      = st.session_state["val_col"]
    sc      = st.session_state["status_col"]
    sec     = st.session_state["sector_col"]
    wvc     = st.session_state["wo_val_col"]
    wsc     = st.session_state["wo_status_col"]
    wec     = st.session_state["wo_sector_col"]

    total_pipeline = _ss(deals[vc]) if vc else 0
    won_val = 0
    if sc:
        won_deals = deals[deals[sc].str.lower().str.contains("won", na=False)]
        if vc: won_val = _ss(won_deals[vc])
    wo_total = len(wo)
    wo_comp  = 0
    if wsc:
        wo_comp_count = wo[wsc].str.lower().str.contains("complet|done|won", na=False).sum()
        wo_comp = wo_comp_count / wo_total * 100 if wo_total > 0 else 0

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1: st.metric("📊 Pipeline Value", _fmt(total_pipeline))
    with c2: st.metric("📁 Active Deals", len(deals))
    with c3: st.metric("🏆 Won Value", _fmt(won_val))
    with c4: st.metric("⚙️ Work Orders", wo_total)
    with c5: st.metric("✅ WO Completion", f"{wo_comp:.0f}%")

    st.divider()

# ── Not connected banner ──────────────────────────────────────────────────
else:
    st.info("👈 Connect your Monday.com token in the sidebar to get started.")

# ── Chat history ──────────────────────────────────────────────────────────
for entry in st.session_state["chat_history"]:
    st.markdown(f'<div class="chat-bubble-user">{entry["q"]}</div>', unsafe_allow_html=True)
    intent_label = INTENT_LABELS.get(entry["intent"], entry["intent"])
    st.markdown(f'<div class="chat-bubble-agent"><span class="intent-badge">{intent_label}</span><br>{entry["md"]}</div>', unsafe_allow_html=True)
    if entry.get("df") is not None:
        st.dataframe(entry["df"], use_container_width=True, hide_index=True)

# ── Query input ───────────────────────────────────────────────────────────
st.divider()

# Handle quick-query button press
pending = st.session_state.pop("_pending_query", None)

with st.form(key="query_form", clear_on_submit=True):
    col_inp, col_btn = st.columns([5, 1])
    with col_inp:
        query_input = st.text_input(
            "Ask a question",
            value=pending or "",
            placeholder="e.g. How's our pipeline looking? | Show at-risk deals | Revenue forecast",
            label_visibility="collapsed"
        )
    with col_btn:
        submitted = st.form_submit_button("Ask →", use_container_width=True)

if submitted and query_input:
    if not st.session_state["connected"]:
        st.warning("⚠️ Please connect to Monday.com first using the sidebar.")
    else:
        deals   = st.session_state["deals"]
        wo      = st.session_state["wo"]
        vc      = st.session_state["val_col"]
        sc      = st.session_state["status_col"]
        sec_col = st.session_state["sector_col"]
        wvc     = st.session_state["wo_val_col"]
        wsc     = st.session_state["wo_status_col"]
        wec     = st.session_state["wo_sector_col"]

        # NLP pipeline
        intent, scores, tokens = classify_intent(query_input)
        entities = extract_entities(query_input)

        # Follow-up context
        ctx = st.session_state["context"]
        if intent == "general" and ctx.get("last_intent"):
            if any(t in tokens for t in ["breakdown","detail","more","sector","stage","drill"]):
                intent = ctx["last_intent"]
                if not entities["sector"]: entities["sector"] = ctx.get("last_sector")
                if not entities["time"]:   entities["time"]   = ctx.get("last_time")

        args = (deals, wo, vc, sc, sec_col, wvc, wsc, wec, entities)

        if intent == "leadership_update":
            empty = {"time": {}, "sector": None, "stage": None, "top_n": None}
            eargs = (deals, wo, vc, sc, sec_col, wvc, wsc, wec, empty)
            p = q_pipeline(*eargs); w = q_work_orders(*eargs); a = q_at_risk(*eargs); f = q_forecast(*eargs)
            md, df_out = fmt_leadership(p, w, a, f)
        elif intent in QUERY_DISPATCH:
            result = QUERY_DISPATCH[intent](*args)
            renderer = RENDER_DISPATCH.get(result["type"], fmt_general)
            md, df_out = renderer(result)
        else:
            result = {"type":"general","deals_count":len(deals),"wo_count":len(wo),"deals_value":_ss(deals[vc]) if vc else 0,"wo_value":_ss(wo[wvc]) if wvc else 0}
            md, df_out = fmt_general(result); intent = "general"

        # Save to history
        st.session_state["chat_history"].append({"q": query_input, "intent": intent, "md": md, "df": df_out})
        st.session_state["context"] = {"last_intent": intent, "last_sector": entities["sector"], "last_time": entities["time"]}
        st.rerun()
