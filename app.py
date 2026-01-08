import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import datetime
from data_sources import load_dashboard_data
from streamlit_gsheets import GSheetsConnection


st.set_page_config(
    page_title="Klaviyo Pulse",
    page_icon="K",
    layout="wide",
)

CHART_CATEGORY_COLORS = [
    "#FFA421",
    "#803DF5",
    "#00C0F2",
    "#2ECC71",
    "#E74C3C",
    "#F39C12",
    "#1ABC9C",
    "#3498DB",
    "#9B59B6",
    "#34495E",
]

BENCHMARKS = {
    "open_rate": 0.432,           # 43.2%
    "click_rate": 0.0125,         # 1.25%
    "bounce_rate": 0.00631,       # 0.631%
    "spam_complaint_rate": 0.0000787,  # 0.00787%
    "unsubscribe_rate": 0.00285,  # 0.285%
}

RATE_METRICS = {
    "open_rate": ("opens", "sends"),
    "click_rate": ("clicks", "sends"),
    "bounce_rate": ("bounced", "sends"),
    "spam_complaint_rate": ("spam_complaints", "sends"),
    "unsubscribe_rate": ("unsubscribes", "sends"),
}

COUNT_METRICS = ["sends", "opens", "clicks", "spam_complaints", "unsubscribes", "bounced"]

def safe_div(a, b):
    b = b.replace(0, pd.NA) if isinstance(b, pd.Series) else (pd.NA if b == 0 else b)
    return a / b

# ---- UI Controls
dimensions = st.multiselect(
    "Dimensions",
    ['type', 'campaign_id', 'name', 'status', 'send_time', 'send_channel', 'account', 'group'],
    default=['account', 'name', 'send_time', 'group', 'campaign_id'],
    max_selections=5,
)

metrics = st.multiselect(
    "Metrics",
    ['open_rate', 'click_rate', 'bounce_rate', 'spam_complaint_rate', 'unsubscribe_rate', 
     'sends', 'opens', 'clicks', 'spam_complaints', 'unsubscribes', 'bounced'],
    default=['open_rate', 'click_rate', 'bounce_rate', 'sends'],
    max_selections=10,
)

yesterday = datetime.date.today() - datetime.timedelta(days=1)
d = st.date_input(
    "Filter", 
    (yesterday - datetime.timedelta(days=30), yesterday),
    # max_value=datetime.date.today(),
    # min_value=yesterday - datetime.timedelta(days=700)
    )
if isinstance(d, tuple) and len(d) == 2 and d[0] and d[1]:
    st.session_state.last_valid_range = d
start = st.session_state.last_valid_range[0].strftime("%Y-%m-%d"+"T00:00:00.00Z")
end = st.session_state.last_valid_range[1].strftime("%Y-%m-%d"+"T23:59:59.59Z")

@st.cache_data(ttl=600, show_spinner="Loading Klaviyo Data...")
def load_dashboard_data_cached(start, end):
    try:
        data = load_dashboard_data(start, end)
        if data is None or not isinstance(data, pd.DataFrame) or data.empty:
            return None
        return data
    except Exception as e:
        return None

raw = load_dashboard_data_cached(start, end)
if raw is None:
    st.warning(
        "Klaviyo is temporarily rate-limiting requests. "
        "Please wait a moment and try again."
    )
    st.stop()  

conn = st.connection("gsheets", type=GSheetsConnection)
group_data = conn.read(ttl=0, worksheet="Sheet1")
data = raw.merge(group_data, how="left", on="campaign_id")

# ---- create sends 
data["sends"] = safe_div(data["opens"], data["open_rate"]).fillna(0)
data.loc[data["sends"] < 0, "sends"] = 0


# ---- aggregate
if len(dimensions) == 0:
    st.warning("Please select at least 1 dimension.")
    st.stop()
    
data = data.groupby(dimensions, dropna=False, as_index=False).agg({
    "sends": "sum",
    "opens": "sum",
    "clicks": "sum",
    "spam_complaints": "sum",
    "unsubscribes": "sum",
    "bounced": "sum",
    "average_order_value": "mean",
})
data["open_rate"] = safe_div(data["opens"], data["sends"]).fillna(0) * 100
data["click_rate"] = safe_div(data["clicks"], data["sends"]).fillna(0) * 100
data["spam_complaint_rate"] = safe_div(data["spam_complaints"], data["sends"]).fillna(0) * 100
data["unsubscribe_rate"] = safe_div(data["unsubscribes"], data["sends"]).fillna(0) * 100
data["bounce_rate"] = safe_div(data["bounced"], data["sends"]).fillna(0) * 100
data["sends"] = data["sends"].round().astype(int)
if 'group' in data.columns:
    data['group'] = data['group'].fillna('default group')
data.fillna(0, inplace=True)

st.divider()

def metric_summary(metric: str):
    # counts
    if metric in COUNT_METRICS:
        return (int(data[metric].sum()), "no benchmark", "off", "normal")

    # rates
    if metric in RATE_METRICS:
        num_col, den_col = RATE_METRICS[metric]
        actual = float(safe_div(data[num_col].sum(), data[den_col].sum()) or 0.0)
        bench = BENCHMARKS.get(metric)

        value = f"{actual * 100:.2f}%"

        if bench:
            pct_vs = (actual / bench - 1) * 100
            high_low = "higher" if pct_vs > 0 else "lower"
            compare = f"{pct_vs:.0f}% {high_low} than benchmark: {bench*100:.3f}%"
            return (value, compare, "auto", "normal")

        return (value, "no benchmark", "off", "normal")

    return (0, "no benchmark", "off", "normal")


def scorecard_breakdown(dimension: str, metric: str):
    if metric in COUNT_METRICS:
        t = data.groupby(dimension, dropna=False)[metric].sum().reset_index()
        t = t.rename(columns={metric: "value"})
        return t

    if metric in RATE_METRICS:
        num_col, den_col = RATE_METRICS[metric]
        t = data.groupby(dimension, dropna=False).agg({num_col: "sum", den_col: "sum"}).reset_index()
        t["value"] = safe_div(t[num_col], t[den_col]).fillna(0)
        return t

    return pd.DataFrame(columns=[dimension, "value"])

def bar_chart(t: pd.DataFrame, y_col: str):
    # if metric is a rate (0-1), show percentage on axis
    is_rate = t["value"].max() <= 1.0 and t["value"].min() >= 0.0
    x = (t["value"] * 100) if is_rate else t["value"]

    fig = go.Figure(go.Bar(
        x=x,
        y=t[y_col],
        orientation="h",
        marker=dict(color=CHART_CATEGORY_COLORS),
    ))
    fig.update_layout(
        margin=dict(l=10, r=10, t=10, b=10),
        height=220,
    )
    return fig

        
cols = st.columns(4)
for i in range(min(4, len(metrics))):
    m = metrics[i]
    val, cmp, arrow, color = metric_summary(m)
    cols[i].metric(m, val, cmp, delta_arrow=arrow, delta_color=color, border=True)

    dim0 = dimensions[0]
    if data[dim0].nunique() < 10:
        t = scorecard_breakdown(dim0, m)
        cols[i].plotly_chart(bar_chart(t, dim0), config={"displayModeBar": False})


# ---- table
if "send_time" in data.columns:
    data = data.sort_values(by='send_time', ascending=False)
    
edited_data = st.data_editor(
    data = data[dimensions + metrics], 
    hide_index = True, 
    height=570,   
    column_config={
        "campaign_id": st.column_config.TextColumn(disabled=True),
        "name": st.column_config.TextColumn(disabled=True),
        "send_time": st.column_config.DatetimeColumn(disabled=True, format="D MMM YYYY, h:mm a",),
        "account": st.column_config.MultiselectColumn(options=data['account'].unique().tolist() if "account" in data.columns else [], disabled=True, color=CHART_CATEGORY_COLORS),
        "group": st.column_config.TextColumn(disabled=False),
        "open_rate": st.column_config.NumberColumn(format="%.2f%%", disabled=True),
        "click_rate": st.column_config.NumberColumn(format="%.2f%%", disabled=True),
        "bounce_rate": st.column_config.NumberColumn(format="%.2f%%", disabled=True),
        "unsubscribe_rate": st.column_config.NumberColumn(format="%.3f%%", disabled=True),
        "spam_complaint_rate": st.column_config.NumberColumn(format="%.4f%%", disabled=True),
        "sends": st.column_config.TextColumn(disabled=True),
        "opens": st.column_config.TextColumn(disabled=True),
        "clicks": st.column_config.TextColumn(disabled=True),
        "spam_complaints": st.column_config.TextColumn(disabled=True),
        "unsubscribes": st.column_config.TextColumn(disabled=True),
        "bounced": st.column_config.TextColumn(disabled=True),
    })

if "group" in dimensions and "campaign_id" in dimensions:
    if st.button("Save Group", type="primary"):
        edited_data = edited_data[['campaign_id', 'group']]
        conn.update(worksheet="Sheet1", data = edited_data)
        st.write('group saved')
    st.write("You can modify the Klaviyo campaigns's group by editing the `group` in the table above☝️")
    st.write("Click this button after you have edited the `group`")
if "group" in dimensions and "campaign_id" not in dimensions:
    st.button("Save Group", type="secondary", disabled=True)
    st.write("Please select `campaign_id` along with `group` if you want to save your changes to `group`")
else:
    pass

prompt = st.chat_input("Ask anything about Klaviyo...")
if prompt:
    st.write(f"AI agent for Klaviyo will come soon...")

    