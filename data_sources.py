import asyncio
import aiohttp
import pandas as pd
import streamlit as st

REVISION = "2025-10-15"
ACCEPT = "application/vnd.api+json"
CONTENT_TYPE = "application/vnd.api+json"


def _headers(api_key: str, is_post: bool = False) -> dict:
    h = {
        "Authorization": f"Klaviyo-API-Key {api_key}",
        "accept": ACCEPT,
        "revision": REVISION,
    }
    if is_post:
        h["content-type"] = CONTENT_TYPE
    return h


async def _request_json(
    session: aiohttp.ClientSession,
    method: str,
    url: str,
    *,
    headers: dict,
    params: dict | None = None,
    json_body: dict | None = None,
    timeout_sec: int = 60,
) -> dict:
    timeout = aiohttp.ClientTimeout(total=timeout_sec)
    async with session.request(
        method,
        url,
        headers=headers,
        params=params,
        json=json_body,
        timeout=timeout,
    ) as resp:
        # raise_for_status equivalent
        if resp.status >= 400:
            text = await resp.text()
            raise RuntimeError(f"{method} {url} -> {resp.status}\n{text}")
        return await resp.json()


async def get_klaviyo_report_async(api_key: str, conversion_metric_id: str, start: str, end: str, session: aiohttp.ClientSession) -> pd.DataFrame:
    url = "https://a.klaviyo.com/api/campaign-values-reports/"
    headers = _headers(api_key, is_post=True)

    payload = {
        "data": {
            "type": "campaign-values-report",
            "attributes": {
                "timeframe": {"start": start, "end": end},
                "conversion_metric_id": conversion_metric_id,
                "statistics": [
                    "bounce_rate", "click_rate", "conversion_rate", "delivery_rate",
                    "open_rate", "spam_complaint_rate", "unsubscribe_rate",
                    "average_order_value", "opens", "clicks", "delivered",
                    "spam_complaints", "unsubscribes", "bounced",
                ],
            },
        }
    }

    rows = []
    next_url = url

    while next_url:
        data = await _request_json(
            session,
            "POST",
            next_url,
            headers=headers,
            json_body=payload,
        )

        results = data["data"]["attributes"]["results"]
        for r in results:
            rows.append({**r["groupings"], **r["statistics"]})

        next_url = data.get("links", {}).get("next")

    return pd.DataFrame(rows)


async def get_campaign_details_async(api_key: str, start: str, end: str, session: aiohttp.ClientSession) -> pd.DataFrame:
    url = "https://a.klaviyo.com/api/campaigns"
    headers = _headers(api_key, is_post=False)
    params = {
        "filter": f"and(equals(messages.channel,'email'),greater-or-equal(scheduled_at,{start}))"
    }

    rows = []
    next_url = url
    next_params = params

    while next_url:
        data = await _request_json(
            session,
            "GET",
            next_url,
            headers=headers,
            params=next_params,
        )

        for r in data.get("data", []):
            rows.append({
                "type": r["type"],
                "campaign_id": r["id"],
                "name": r["attributes"].get("name"),
                "status": r["attributes"].get("status"),
                "archived": r["attributes"].get("archived"),
                "send_time": r["attributes"].get("send_time"),
                "scheduled_at": r["attributes"].get("scheduled_at"),
            })

        next_url = data.get("links", {}).get("next")
        next_params = None  # next link already contains params

    return pd.DataFrame(rows)


async def _load_one_region(region_cfg: dict, start: str, end: str, session: aiohttp.ClientSession) -> pd.DataFrame:
    # Run both endpoints concurrently for this region
    df_campaigns_task = asyncio.create_task(
        get_campaign_details_async(region_cfg["api_key"], start, end, session)
    )
    df_report_task = asyncio.create_task(
        get_klaviyo_report_async(region_cfg["api_key"], region_cfg["pixel"], start, end, session)
    )

    df1, df2 = await asyncio.gather(df_campaigns_task, df_report_task)

    if df1.empty or df2.empty:
        df = pd.DataFrame()
    else:
        df = df1.merge(df2, how="inner", on="campaign_id")

        # keep your original cleanup
        for col in ["campaign_message_id", "archived"]:
            if col in df.columns:
                del df[col]

        df["account"] = region_cfg["name"]

    return df


def get_klaviyo_config():
    cfg = st.secrets["klaviyo"]
    return {
        region: {
            "name": cfg[region]["name"],
            "api_key": cfg[region]["api_key"],
            "pixel": cfg[region]["pixel"],
        }
        for region in cfg
    }

async def load_dashboard_data_async(start: str, end: str) -> pd.DataFrame:
    config = get_klaviyo_config()

    # Limit concurrent TCP connections so you don’t get rate-limited too aggressively
    connector = aiohttp.TCPConnector(limit=20)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            _load_one_region(config[region], start, end, session)
            for region in ["sg", "intl", "au", "hk", "tw"]
        ]
        dfs = await asyncio.gather(*tasks)

    dfs = [d for d in dfs if d is not None and not d.empty]
    
    df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    NUMERIC_COLS = [
        "bounce_rate", "click_rate", "conversion_rate", "delivery_rate", "open_rate",
        "spam_complaint_rate", "unsubscribe_rate",
        "average_order_value",
        "opens", "clicks", "delivered",
        "spam_complaints", "unsubscribes", "bounced",
    ]

    for c in NUMERIC_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
            
    return df


# ----- sync wrapper
def load_dashboard_data(start: str, end: str) -> pd.DataFrame:
    return asyncio.run(load_dashboard_data_async(start, end))


if __name__ == "__main__":
    df = load_dashboard_data("2025-04-01", "2026-01-13")
    df.to_csv('test.csv')