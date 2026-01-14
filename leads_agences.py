# app.py
import os
import requests
import pandas as pd
import streamlit as st
import plotly.express as px
from datetime import datetime, timedelta, timezone

# =========================
# 0) Page config
# =========================
st.set_page_config(
    page_title="100% Immo — Dashboard Leads",
    page_icon="🏠",
    layout="wide",
)

st.title("🏠 100% Immo — Dashboard Leads (Sellsy)")

# =========================
# 1) Secrets / Config
# =========================
# Option A (recommandé) : .streamlit/secrets.toml
# CLIENT_ID="..."
# CLIENT_SECRET="..."
#
# Option B : variables d'environnement
# export SELLSY_CLIENT_ID="..."
# export SELLSY_CLIENT_SECRET="..."

# CLIENT_ID = st.secrets.get("CLIENT_ID", os.getenv("SELLSY_CLIENT_ID", ""))
# CLIENT_SECRET = st.secrets.get("CLIENT_SECRET", os.getenv("SELLSY_CLIENT_SECRET", ""))

CLIENT_ID = st.secrets["CLIENT_ID"]
CLIENT_SECRET = st.secrets["CLIENT_SECRET"]


if not CLIENT_ID or not CLIENT_SECRET:
    st.warning("⚠️ Renseigne CLIENT_ID et CLIENT_SECRET via st.secrets ou variables d'environnement.")
    st.stop()

AUTH_URL = "https://login.sellsy.com/oauth2/access-tokens"
BASE_URL = "https://api.sellsy.com/v2/opportunities"

# =========================
# 2) Sellsy API helpers
# =========================
@st.cache_data(ttl=3500, show_spinner=False)
def get_access_token(client_id: str, client_secret: str) -> str:
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    r = requests.post(AUTH_URL, data=data, timeout=30)
    r.raise_for_status()
    return r.json().get("access_token")

@st.cache_data(ttl=900, show_spinner=True)  # refresh data toutes les 15 min
def fetch_all_opportunities(client_id: str, client_secret: str) -> pd.DataFrame:
    token = get_access_token(client_id, client_secret)
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    all_opps = []
    limit = 100
    offset = 0

    while True:
        params = {"limit": limit, "offset": offset, "direction": "asc"}
        r = requests.get(BASE_URL, headers=headers, params=params, timeout=60)
        r.raise_for_status()
        payload = r.json()

        opps = payload.get("data", [])
        all_opps.extend(opps)

        pagination = payload.get("pagination", {})
        total = int(pagination.get("total", 0) or 0)
        count = int(pagination.get("count", 0) or 0)
        offset += count

        if count == 0 or offset >= total:
            break

    df = pd.json_normalize(all_opps)
    return df

# =========================
# 3) Business logic (mappings)
# =========================
STEP_GROUP_MAP = {
    # Regroup: relances/transferts
    "Relance Opp": "Relance / Transfert",
    "Relance opp": "Relance / Transfert",
    "Transfert Opp": "Relance / Transfert",
    "Trnasfert Opp": "Relance / Transfert",
    "Transfert opp": "Relance / Transfert",

    # Regroup: devis signé
    "Devis Signé": "Devis signé",
    "DEVIS SIGNÉ ANT ET ROMY": "Devis signé",
    "Devis signé": "Devis signé",

    # Regroup: négo
    "Négo/Envoi devis": "Négociation",
    "Devis/Négociation": "Négociation",

    # Regroup: visio/rdv
    "VISIO/RDV": "VISIO / RDV",
    "RDV/Visio": "VISIO / RDV",
    "Visio / RDV": "VISIO / RDV",

    # Regroup: premier contact
    "Premier appel Tel": "Premier contact",
    "Premier contact téléphonique": "Premier contact",
    "Contact tél/Mail": "Premier contact",

    # Regroup: refus
    "Refus Timing": "Refus",
    "Refus Timing à rappeler": "Refus",
    "Refus Argent": "Refus",
    "Refus Offre": "Refus",

    # Les autres restent tels quels (Piste, NRP, Prise en charge lead, Envoi de devis, etc.)
}

WORKFLOW_ORDER = [
    "Piste",
    "Prise en charge lead",
    "NRP / Premier contact",   # étape synthèse (NRP ou Premier contact)
    "VISIO / RDV",
    "Envoi de devis",
    "Refus / Devis signé",
]

def normalize_steps(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # sécurise les colonnes
    if "step.name" not in df.columns:
        df["step.name"] = None
    if "pipeline.name" not in df.columns:
        df["pipeline.name"] = None
    if "status" not in df.columns:
        df["status"] = None
    if "created" not in df.columns:
        df["created"] = None

    # dates
    df["created_dt"] = pd.to_datetime(df["created"], errors="coerce", utc=True)
    df["created_date"] = df["created_dt"].dt.date

    # pipeline (commercial)
    df["pipeline_clean"] = df["pipeline.name"].fillna("Inconnu")

    # step group
    raw_step = df["step.name"].fillna("Inconnu")
    df["step_group"] = raw_step.map(lambda x: STEP_GROUP_MAP.get(x, x))

    # workflow stage (selon ton ordre)
    def to_workflow_stage(step_group: str) -> str:
        s = (step_group or "").strip()

        if s == "Piste":
            return "Piste"
        if s == "Prise en charge lead":
            return "Prise en charge lead"
        if s == "NRP":
            return "NRP / Premier contact"
        if s == "Premier contact":
            return "NRP / Premier contact"
        if s == "VISIO / RDV":
            return "VISIO / RDV"
        if s == "Envoi de devis":
            return "Envoi de devis"
        if s == "Refus":
            return "Refus / Devis signé"
        if s == "Devis signé":
            return "Refus / Devis signé"

        # Si un step est "Relance / Transfert" : en pratique c'est du milieu de pipeline.
        # On le classe juste avant VISIO/RDV (modifiable si tu préfères ailleurs).
        if s == "Relance / Transfert":
            return "NRP / Premier contact"

        # fallback
        return "Autre"

    df["workflow_stage"] = df["step_group"].map(to_workflow_stage)
    return df

# =========================
# 4) Load data
# =========================
with st.spinner("🔄 Récupération des opportunités Sellsy..."):
    df_raw = fetch_all_opportunities(CLIENT_ID, CLIENT_SECRET)

df = normalize_steps(df_raw)

# Exclure Prospection GLOBAL
df = df[df["pipeline_clean"] != "Pipeline Prospection GLOBAL"].copy()

# =========================
# 5) Sidebar filters
# =========================
st.sidebar.header("🔎 Filtres")

# Time windows
period = st.sidebar.radio(
    "Période",
    ["7 derniers jours", "30 derniers jours", "Année en cours", "Tout"],
    index=1
)

now_utc = datetime.now(timezone.utc)

if period == "7 derniers jours":
    start_dt = now_utc - timedelta(days=7)
elif period == "30 derniers jours":
    start_dt = now_utc - timedelta(days=30)
elif period == "Année en cours":
    start_dt = datetime(now_utc.year, 1, 1, tzinfo=timezone.utc)
else:
    start_dt = None

df_f = df.copy()
if start_dt is not None:
    df_f = df_f[df_f["created_dt"] >= start_dt].copy()

pipelines = sorted([p for p in df["pipeline_clean"].dropna().unique().tolist() if p != "Pipeline Prospection GLOBAL"])
pipeline_selected = st.sidebar.multiselect("Commerciaux (pipeline)", pipelines, default=pipelines)

df_f = df_f[df_f["pipeline_clean"].isin(pipeline_selected)].copy()

status_list = sorted(df["status"].dropna().unique().tolist())
status_selected = st.sidebar.multiselect("Status", status_list, default=status_list)
df_f = df_f[df_f["status"].isin(status_selected)].copy()

# =========================
# 6) KPIs
# =========================
total_leads = len(df_f)
won = int((df_f["status"] == "won").sum())
lost = int((df_f["status"] == "lost").sum())
closed = int((df_f["status"] == "closed").sum())
open_ = int((df_f["status"] == "open").sum())
late = int((df_f["status"] == "late").sum())

won_rate = (won / total_leads * 100) if total_leads else 0.0
lost_rate = (lost / total_leads * 100) if total_leads else 0.0

kpi1, kpi2, kpi3, kpi4, kpi5, kpi6 = st.columns(6)
kpi1.metric("Leads", f"{total_leads:,}".replace(",", " "))
kpi2.metric("Won", f"{won:,}".replace(",", " "), f"{won_rate:.1f}%")
kpi3.metric("Lost", f"{lost:,}".replace(",", " "), f"{lost_rate:.1f}%")
kpi4.metric("Open", f"{open_:,}".replace(",", " "))
kpi5.metric("Closed", f"{closed:,}".replace(",", " "))
kpi6.metric("Late", f"{late:,}".replace(",", " "))

st.divider()

# =========================
# 7) Graphs — Vue globale
# =========================
c1, c2 = st.columns([2, 1])

with c1:
    st.subheader("📈 Leads créés dans le temps")
    ts = (
        df_f.dropna(subset=["created_dt"])
        .assign(day=lambda x: x["created_dt"].dt.date)
        .groupby("day", as_index=False)
        .size()
        .rename(columns={"size": "leads"})
    )
    fig_ts = px.line(ts, x="day", y="leads", markers=True)
    st.plotly_chart(fig_ts, use_container_width=True)

with c2:
    st.subheader("📌 Répartition par status")
    by_status = df_f.groupby("status", as_index=False).size().rename(columns={"size": "count"})
    fig_status = px.pie(by_status, names="status", values="count", hole=0.45)
    st.plotly_chart(fig_status, use_container_width=True)

st.divider()

# =========================
# 8) Leads par commercial (pipeline)
# =========================
st.subheader("👤 Leads par commercial (pipeline)")
by_pipeline = (
    df_f.groupby("pipeline_clean", as_index=False)
    .size()
    .rename(columns={"size": "leads"})
    .sort_values("leads", ascending=False)
)
fig_pipe = px.bar(by_pipeline, x="pipeline_clean", y="leads")
fig_pipe.update_layout(xaxis_title="Commercial (pipeline)", yaxis_title="Leads")
st.plotly_chart(fig_pipe, use_container_width=True)

st.divider()

# =========================
# 9) Steps regroupés + workflow funnel
# =========================
c3, c4 = st.columns(2)

with c3:
    st.subheader("🧩 Répartition par step (regroupé)")
    by_step = (
        df_f.groupby("step_group", as_index=False)
        .size()
        .rename(columns={"size": "count"})
        .sort_values("count", ascending=False)
    )
    fig_step = px.bar(by_step, x="count", y="step_group", orientation="h")
    fig_step.update_layout(xaxis_title="Leads", yaxis_title="Step (regroupé)")
    st.plotly_chart(fig_step, use_container_width=True)

with c4:
    st.subheader("🪜 Funnel workflow (ordre métier)")
    wf = (
        df_f.groupby("workflow_stage", as_index=False)
        .size()
        .rename(columns={"size": "count"})
    )

    # impose l'ordre
    order = WORKFLOW_ORDER + ["Autre"]
    wf["workflow_stage"] = pd.Categorical(wf["workflow_stage"], categories=order, ordered=True)
    wf = wf.sort_values("workflow_stage")

    fig_wf = px.funnel(wf, x="count", y="workflow_stage")
    fig_wf.update_layout(xaxis_title="Leads", yaxis_title="Étape workflow")
    st.plotly_chart(fig_wf, use_container_width=True)

st.divider()

# =========================
# 10) Tableau détail + export
# =========================
st.subheader("🧾 Détails")
cols_default = [
    "id", "number", "name",
    "created_dt", "status",
    "pipeline_clean",
    "step.name", "step_group", "workflow_stage",
    "probability",
    "amount.value", "amount.currency",
    "source.name",
]
cols_existing = [c for c in cols_default if c in df_f.columns]
df_show = df_f[cols_existing].copy().sort_values("created_dt", ascending=False)

st.dataframe(df_show, use_container_width=True, height=420)

csv = df_show.to_csv(index=False).encode("utf-8")
st.download_button("⬇️ Télécharger (CSV)", data=csv, file_name="100immo_leads_export.csv", mime="text/csv")
