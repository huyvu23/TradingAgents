"""Vietnam stock market (.VN) data vendor for HOSE, HNX, and UPCoM.

Supplements Yahoo Finance for Vietnamese equities (``.VN``):
1. Vietnamese financial news via Google News RSS Vietnam (CafeF, Vietstock,
   VnExpress, Tin nhanh chứng khoán, Báo Đầu Tư, etc.), filtered by the
   analysis window (``in_window``) to prevent look-ahead bias.
2. Vietnam macroeconomic news (SBV interest rates, USD/VND exchange rate,
   VN-Index liquidity, foreign capital flows, FTSE/MSCI market upgrade).
3. OHLCV price fallback via DNSE Entrade chart API for HNX and UPCoM tickers
   (e.g. ``PVS.VN``, ``ACV.VN``, ``BSR.VN``) that Yahoo Finance does not cover.
4. Company identity, live order-flow & foreign investor net buy/sell
   (Giao dịch Khối ngoại & Mua/Bán chủ động) via SSI iBoard, plus local
   fundamental ratios & business profile via Simplize.
"""

from __future__ import annotations

import contextlib
import html
import json
import logging
import re
import xml.etree.ElementTree as ET
from datetime import UTC, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import quote_plus
from urllib.request import Request, urlopen

import pandas as pd

from tradingagents.dataflows.date_window import in_window, is_historical
from tradingagents.dataflows.symbols import normalize_symbol

logger = logging.getLogger(__name__)

_VN_TZ = timezone(timedelta(hours=7))
_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko)"

_SSI_STOCK_URL = "https://iboard-query.ssi.com.vn/stock/{symbol}"
_SSI_HEADERS = {
    "User-Agent": _UA,
    "Accept": "application/json",
    "Referer": "https://iboard.ssi.com.vn/",
    "Origin": "https://iboard.ssi.com.vn",
}

_SIMPLIZE_SUMMARY_URL = "https://api.simplize.vn/api/company/summary/{symbol}"
_ENTRADE_OHLC_URL = (
    "https://services.entrade.com.vn/chart-api/v2/ohlcs/stock"
    "?from={start_ts}&to={end_ts}&symbol={symbol}&resolution=1D"
)
_GOOGLE_NEWS_VN_RSS = (
    "https://news.google.com/rss/search?q={query}&hl=vi&gl=VN&ceid=VN:vi"
)

DEFAULT_VN_GLOBAL_NEWS_QUERIES = [
    "VN-Index chứng khoán Việt Nam dòng tiền khối ngoại",
    "Ngân hàng Nhà nước lãi suất tỷ giá USD VND",
    "nâng hạng thị trường chứng khoán Việt Nam FTSE MSCI",
    "kinh tế vĩ mô Việt Nam tăng trưởng GDP FDI xuất khẩu",
    "trái phiếu doanh nghiệp bất động sản tín dụng ngân hàng",
]


def is_vn_ticker(ticker: str) -> bool:
    """Return True when ``ticker`` is a Vietnam-listed symbol (``.VN``)."""
    if not isinstance(ticker, str):
        return False
    return normalize_symbol(ticker).upper().endswith(".VN")


def vn_base_symbol(ticker: str) -> str:
    """Return the bare Vietnam stock code without the ``.VN`` suffix."""
    canonical = normalize_symbol(ticker).upper()
    if canonical.endswith(".VN"):
        return canonical[:-3]
    return canonical


def _strip_html_tags(text: str | None) -> str:
    if not text:
        return ""
    cleaned = re.sub(r"<[^>]+>", " ", str(text))
    return " ".join(html.unescape(cleaned).split())


def _http_get_json(url: str, headers: dict[str, str] | None = None, timeout: float = 8.0) -> dict | None:
    req_headers = {"User-Agent": _UA, "Accept": "application/json"}
    if headers:
        req_headers.update(headers)
    req = Request(url, headers=req_headers)
    try:
        with urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8", errors="replace"))
    except Exception as exc:  # noqa: BLE001
        logger.debug("VN market JSON fetch failed for %s: %s", url, exc)
        return None


def fetch_ssi_stock_snapshot(ticker: str, timeout: float = 8.0) -> dict:
    """Fetch real-time stock metadata, price bands, active buy/sell & foreign flow from SSI iBoard."""
    base = vn_base_symbol(ticker)
    payload = _http_get_json(_SSI_STOCK_URL.format(symbol=base), headers=_SSI_HEADERS, timeout=timeout)
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        return payload["data"]
    return {}


def fetch_simplize_summary(ticker: str, timeout: float = 8.0) -> dict:
    """Fetch company summary, valuation ratios, growth metrics and business profile from Simplize."""
    base = vn_base_symbol(ticker)
    payload = _http_get_json(_SIMPLIZE_SUMMARY_URL.format(symbol=base), timeout=timeout)
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        return payload["data"]
    return {}


def get_vn_company_profile(ticker: str) -> dict:
    """Resolve deterministic identity metadata for a ``.VN`` ticker across HOSE, HNX, and UPCoM."""
    ssi = fetch_ssi_stock_snapshot(ticker)
    simplize = fetch_simplize_summary(ticker)

    name_vi = (
        ssi.get("companyNameVi")
        or ssi.get("clientName")
        or simplize.get("nameVi")
        or simplize.get("name")
    )
    name_en = (
        ssi.get("companyNameEn")
        or ssi.get("clientNameEn")
        or simplize.get("nameEn")
    )
    if name_vi and name_en and name_vi.lower() != name_en.lower():
        long_name = f"{name_vi} ({name_en})"
    else:
        long_name = name_vi or name_en

    exchange = (
        (ssi.get("exchange") or simplize.get("stockExchange") or "").upper()
    )
    if exchange == "UPCOM":
        exchange = "UPCoM"

    profile: dict[str, Any] = {}
    if long_name:
        profile["longName"] = long_name
        profile["shortName"] = name_vi or name_en
    if simplize.get("bcEconomicSectorName"):
        profile["sector"] = simplize["bcEconomicSectorName"]
    if simplize.get("industryActivity"):
        profile["industry"] = simplize["industryActivity"]
    if exchange:
        profile["exchange"] = exchange
    profile["currency"] = "VND"
    profile["financialCurrency"] = "VND"
    return profile


def get_vn_ohlcv_df(
    ticker: str,
    start_date: str,
    end_date: str,
    timeout: float = 10.0,
) -> pd.DataFrame:
    """Fetch daily OHLCV DataFrame for a ``.VN`` ticker from DNSE Entrade.

    Supports HOSE, HNX, and UPCoM stocks. Converts Entrade's thousand-VND
    stock prices to full VND to match Yahoo Finance's ``.VN`` unit convention.
    """
    base = vn_base_symbol(ticker)
    start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=_VN_TZ)
    end_dt = (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).replace(tzinfo=_VN_TZ)

    url = _ENTRADE_OHLC_URL.format(
        start_ts=int(start_dt.timestamp()),
        end_ts=int(end_dt.timestamp()),
        symbol=quote_plus(base),
    )
    payload = _http_get_json(url, timeout=timeout)
    if not isinstance(payload, dict) or not payload.get("t"):
        return pd.DataFrame()

    timestamps = payload.get("t") or []
    opens = payload.get("o") or []
    highs = payload.get("h") or []
    lows = payload.get("l") or []
    closes = payload.get("c") or []
    volumes = payload.get("v") or []

    n = min(len(timestamps), len(opens), len(highs), len(lows), len(closes), len(volumes))
    if n == 0:
        return pd.DataFrame()

    dates = [
        datetime.fromtimestamp(int(ts), tz=_VN_TZ).strftime("%Y-%m-%d")
        for ts in timestamps[:n]
    ]
    df = pd.DataFrame(
        {
            "Date": pd.to_datetime(dates),
            "Open": pd.to_numeric(opens[:n], errors="coerce"),
            "High": pd.to_numeric(highs[:n], errors="coerce"),
            "Low": pd.to_numeric(lows[:n], errors="coerce"),
            "Close": pd.to_numeric(closes[:n], errors="coerce"),
            "Volume": pd.to_numeric(volumes[:n], errors="coerce"),
        }
    )
    df = df.dropna(subset=["Date", "Close"]).drop_duplicates(subset=["Date"], keep="last")
    df = df.sort_values("Date").reset_index(drop=True)

    # Entrade quotes Vietnam stocks in thousands of VND (e.g. 71.4 -> 71,400 VND).
    # Multiply by 1000 when median close is below 2,000 so units match Yahoo (.VN).
    if not df.empty and float(df["Close"].median()) < 2000.0:
        for col in ("Open", "High", "Low", "Close"):
            df[col] = (df[col] * 1000.0).round(2)

    df["Adj Close"] = df["Close"]
    return df


def get_vn_fundamentals_enrichment(ticker: str, as_of_date: str | None = None) -> str:
    """Build Vietnam-specific fundamental, valuation, and foreign order-flow block.

    Withheld on historical backtest dates because SSI iBoard and Simplize
    serve present-day snapshots with no historical vintage.
    """
    if is_historical(as_of_date):
        return ""

    ssi = fetch_ssi_stock_snapshot(ticker)
    simplize = fetch_simplize_summary(ticker)
    if not ssi and not simplize:
        return ""

    base = vn_base_symbol(ticker)
    lines: list[str] = [f"## Vietnam Market Local Data & Order Flow ({base}.VN)"]

    exchange = (ssi.get("exchange") or simplize.get("stockExchange") or "").upper()
    if exchange == "UPCOM":
        exchange = "UPCoM"
    name_vi = ssi.get("companyNameVi") or simplize.get("nameVi")
    name_en = ssi.get("companyNameEn") or simplize.get("nameEn")
    if name_vi:
        lines.append(f"- **Tên doanh nghiệp (VN)**: {name_vi}")
    if name_en:
        lines.append(f"- **Tên tiếng Anh**: {name_en}")
    if exchange:
        band = "±7%" if exchange == "HOSE" else ("±10%" if exchange == "HNX" else "±15%")
        lines.append(f"- **Sàn niêm yết**: {exchange} (Biên độ dao động ngày: {band}, Chu kỳ thanh toán: T+2.5, Lô chuẩn: 100 cp)")
    if simplize.get("bcEconomicSectorName") or simplize.get("industryActivity"):
        lines.append(
            f"- **Ngành nghề (Simplize)**: {simplize.get('bcEconomicSectorName') or 'N/A'} / "
            f"{simplize.get('industryActivity') or 'N/A'}"
        )

    # Price band & current session snapshot from SSI iBoard
    ref_p = ssi.get("refPrice") or simplize.get("priceReferrance")
    ceil_p = ssi.get("ceiling") or simplize.get("priceCeiling")
    floor_p = ssi.get("floor") or simplize.get("priceFloor")
    match_p = ssi.get("matchedPrice") or simplize.get("priceClose")
    if ref_p and ceil_p and floor_p:
        lines.append(
            f"- **Mốc giá phiên hiện tại (VND)**: Tham chiếu: {ref_p:,.0f} | "
            f"Trần (Ceiling): {ceil_p:,.0f} | Sàn (Floor): {floor_p:,.0f}"
            + (f" | Thị giá khớp: {match_p:,.0f}" if match_p else "")
        )

    # Foreign investor flow (Khối ngoại)
    buy_f_qty = ssi.get("buyForeignQtty")
    sell_f_qty = ssi.get("sellForeignQtty")
    buy_f_val = ssi.get("buyForeignValue")
    sell_f_val = ssi.get("sellForeignValue")
    remain_f_qty = ssi.get("remainForeignQtty")
    if buy_f_qty is not None and sell_f_qty is not None:
        net_qty = int(buy_f_qty) - int(sell_f_qty)
        net_val = (float(buy_f_val or 0) - float(sell_f_val or 0)) / 1e9
        status_label = "MUA RÒNG" if net_qty > 0 else ("BÁN RÒNG" if net_qty < 0 else "CÂN BẰNG")
        lines.append(
            f"- **Giao dịch Khối ngoại (Foreign Flow - Phiên hiện tại)**: **{status_label}** "
            f"| Mua: {int(buy_f_qty):,} cp | Bán: {int(sell_f_qty):,} cp "
            f"| Chênh lệch ròng: {net_qty:+,} cp ({net_val:+.2f} tỷ VND)"
            + (f" | Room ngoại còn lại: {int(remain_f_qty):,} cp" if remain_f_qty is not None else "")
        )

    # Active Buy vs Active Sell volume (Mua/Bán chủ động)
    bu_vol = ssi.get("stockBUVol")
    sd_vol = ssi.get("stockSDVol")
    total_vol = ssi.get("nmTotalTradedQty") or ssi.get("stockVol")
    if bu_vol is not None and sd_vol is not None and (bu_vol + sd_vol) > 0:
        bu_pct = 100.0 * float(bu_vol) / float(bu_vol + sd_vol)
        lines.append(
            f"- **Dòng tiền Chủ động (Active Order Flow)**: Mua chủ động (BU): {int(bu_vol):,} cp ({bu_pct:.1f}%) "
            f"| Bán chủ động (SD): {int(sd_vol):,} cp ({100.0 - bu_pct:.1f}%)"
            + (f" | Tổng KL khớp lệnh: {int(total_vol):,} cp" if total_vol else "")
        )

    # Simplize valuation & growth metrics
    val_fields = [
        ("Vốn hóa thị trường (Market Cap)", f"{simplize['marketCap']:,.0f} VND" if simplize.get("marketCap") else None),
        ("Tỷ lệ cổ phiếu tự do chuyển nhượng (Free Float)", f"{simplize['freeFloatRate']}%" if simplize.get("freeFloatRate") is not None else None),
        ("P/E (TTM)", simplize.get("peRatio")),
        ("P/B", simplize.get("pbRatio")),
        ("EV/EBITDA", simplize.get("evEbitdaRatio")),
        ("EPS (VND)", f"{simplize['epsRatio']:,.0f}" if simplize.get("epsRatio") else None),
        ("Book Value (VND/cp)", f"{simplize['bookValue']:,.0f}" if simplize.get("bookValue") else None),
        ("ROE (%)", f"{simplize['roe']:.2f}%" if simplize.get("roe") is not None else None),
        ("ROA (%)", f"{simplize['roa']:.2f}%" if simplize.get("roa") is not None else None),
        ("Beta (5Y)", simplize.get("beta5y")),
        ("Tăng trưởng Doanh thu LTM (%)", f"{simplize['revenueLtmGrowth']:+.2f}%" if simplize.get("revenueLtmGrowth") is not None else None),
        ("Tăng trưởng Lợi nhuận ròng LTM (%)", f"{simplize['netIncomeLtmGrowth']:+.2f}%" if simplize.get("netIncomeLtmGrowth") is not None else None),
        ("Tăng trưởng Doanh thu QoQ (%)", f"{simplize['revenueGrowthQoq']:+.2f}%" if simplize.get("revenueGrowthQoq") is not None else None),
        ("Tăng trưởng Lợi nhuận ròng QoQ (%)", f"{simplize['netIncomeGrowthQoq']:+.2f}%" if simplize.get("netIncomeGrowthQoq") is not None else None),
        (
            "Điểm xếp hạng Simplize (thang 5)",
            f"Định giá: {simplize.get('valuationPoint')}/5 | Tăng trưởng: {simplize.get('growthPoint')}/5 | "
            f"Sức khỏe tài chính: {simplize.get('financialHealthPoint')}/5 | Cổ tức: {simplize.get('dividendPoint')}/5"
            if simplize.get("valuationPoint") is not None else None,
        ),
    ]
    for label, val in val_fields:
        if val is not None:
            lines.append(f"- **{label}**: {val}")

    for section_label, raw_html in (
        ("Lĩnh vực kinh doanh chính", simplize.get("mainService") or simplize.get("businessLine")),
        ("Chiến lược kinh doanh", simplize.get("businessStrategy")),
        ("Rủi ro kinh doanh đặc thù", simplize.get("businessRisk")),
    ):
        cleaned = _strip_html_tags(raw_html)
        if cleaned:
            lines.append(f"- **{section_label}**: {cleaned}")

    with contextlib.suppress(Exception):
        from .dnse_mcp import fetch_dnse_mcp_enrichment

        dnse_block = fetch_dnse_mcp_enrichment(ticker)
        if dnse_block:
            lines.append("")
            lines.append(dnse_block)

    return "\n".join(lines)


def _fetch_google_news_vn_rss(
    query: str,
    start_dt: datetime,
    end_dt: datetime,
    limit: int = 20,
    timeout: float = 10.0,
) -> list[dict]:
    """Fetch and date-filter Vietnamese news articles from Google News RSS Vietnam."""
    after_str = (start_dt - timedelta(days=1)).strftime("%Y-%m-%d")
    before_str = (end_dt + timedelta(days=1)).strftime("%Y-%m-%d")
    full_query = f"{query} after:{after_str} before:{before_str}"
    url = _GOOGLE_NEWS_VN_RSS.format(query=quote_plus(full_query))

    req = Request(url, headers={"User-Agent": _UA})
    try:
        with urlopen(req, timeout=timeout) as resp:
            raw_xml = resp.read()
        root = ET.fromstring(raw_xml)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Google News VN RSS fetch failed for %r: %s", query, exc)
        return []

    articles: list[dict] = []
    seen_titles: set[str] = set()
    for item in root.findall("./channel/item"):
        title = (item.findtext("title") or "").strip()
        if not title or title in seen_titles:
            continue
        pub_str = item.findtext("pubDate")
        pub_date = None
        if pub_str:
            with contextlib.suppress(ValueError, TypeError):
                pub_date = parsedate_to_datetime(pub_str).astimezone(UTC)
        if not in_window(pub_date, start_dt, end_dt):
            continue

        source_el = item.find("source")
        publisher = (source_el.text.strip() if source_el is not None and source_el.text else "Google News VN")
        # Strip trailing " - Publisher" from title if duplicated
        suffix = f" - {publisher}"
        if title.endswith(suffix):
            title = title[: -len(suffix)].strip()

        desc = _strip_html_tags(item.findtext("description"))
        if desc and (desc == title or desc.startswith(title)):
            desc = ""

        link = (item.findtext("link") or "").strip()
        seen_titles.add(title)
        articles.append(
            {
                "title": title,
                "summary": desc,
                "publisher": publisher,
                "link": link,
                "pub_date": pub_date,
            }
        )
        if len(articles) >= limit:
            break

    return articles


def get_vn_news(
    ticker: str,
    start_date: str,
    end_date: str,
    limit: int = 20,
) -> list[dict]:
    """Fetch Vietnamese financial news articles for a ``.VN`` ticker within ``[start_date, end_date]``."""
    base = vn_base_symbol(ticker)
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    # Build a targeted query combining stock code and short company name when known
    simplize = fetch_simplize_summary(ticker, timeout=5.0)
    ssi = fetch_ssi_stock_snapshot(ticker, timeout=5.0) if not simplize else {}
    raw_name = (
        simplize.get("nameVi")
        or ssi.get("companyNameVi")
        or simplize.get("nameEn")
        or ""
    )
    # Clean common legal prefixes in Vietnamese company names for better search recall
    short_brand = re.sub(
        r"^(Tổng\s+)?(Công\s+ty\s+)?(Cổ\s+phần\s+|CP\s+|Tập\s+đoàn\s+|TMCP\s+|Đầu\s+tư\s+)+",
        "",
        raw_name,
        flags=re.IGNORECASE,
    ).strip()

    if short_brand and len(short_brand) >= 3 and short_brand.upper() != base:
        query = f'("cổ phiếu {base}" OR "{base}" OR "{short_brand}") chứng khoán'
    else:
        query = f'"cổ phiếu {base}" OR "{base} chứng khoán"'

    articles: list[dict] = []
    seen_titles: set[str] = set()

    # Google News RSS ranks by relevance rather than recency, so a wide window
    # (e.g. 30 days) can crowd out breaking news from the last few days. Fetch
    # the most recent 7-day slice first, then backfill from the full window.
    recent_start = max(start_dt, end_dt - timedelta(days=7))
    if recent_start > start_dt:
        for art in _fetch_google_news_vn_rss(query, recent_start, end_dt, limit=limit):
            if art["title"] not in seen_titles:
                seen_titles.add(art["title"])
                articles.append(art)

    for art in _fetch_google_news_vn_rss(query, start_dt, end_dt, limit=limit):
        if art["title"] not in seen_titles:
            seen_titles.add(art["title"])
            articles.append(art)

    if not articles:
        # Broaden slightly if the strict query had no hits in the window
        fallback_query = f"{base} chứng khoán Việt Nam"
        articles = _fetch_google_news_vn_rss(fallback_query, start_dt, end_dt, limit=limit)

    articles.sort(
        key=lambda a: a.get("pub_date") or datetime.min.replace(tzinfo=UTC),
        reverse=True,
    )
    return articles[:limit]


def get_vn_global_news_articles(
    as_of_date: str,
    look_back_days: int = 7,
    limit: int = 10,
    queries: list[str] | None = None,
) -> list[dict]:
    """Fetch Vietnam macroeconomic & market headlines within the lookback window."""
    curr_dt = datetime.strptime(as_of_date, "%Y-%m-%d")
    start_dt = curr_dt - timedelta(days=look_back_days)
    search_queries = queries or DEFAULT_VN_GLOBAL_NEWS_QUERIES

    collected: list[dict] = []
    seen_titles: set[str] = set()
    per_query_limit = max(3, (limit // max(1, len(search_queries))) + 2)

    for q in search_queries:
        found = _fetch_google_news_vn_rss(q, start_dt, curr_dt, limit=per_query_limit)
        for art in found:
            if art["title"] not in seen_titles:
                seen_titles.add(art["title"])
                collected.append(art)

    collected.sort(
        key=lambda a: a.get("pub_date") or datetime.min.replace(tzinfo=UTC),
        reverse=True,
    )
    return collected[:limit]


def fetch_vn_market_sentiment(
    ticker: str,
    limit: int = 20,
    start_date: str | None = None,
    end_date: str | None = None,
) -> str:
    """Return a structured Vietnamese retail/institutional sentiment & order-flow block for ``.VN`` tickers.

    Replaces the 404 StockTwits stream for Vietnam equities with:
    1. Live Foreign Investor Net Flow (Khối ngoại Mua/Bán ròng) and Active Buy/Sell
       Volume split (Mua chủ động BU vs Bán chủ động SD) from SSI iBoard (when not a
       historical backtest date).
    2. Recent Vietnamese financial media & forum headlines (CafeF, Vietstock,
       VnExpress, Tin nhanh chứng khoán, FireAnt, F319) within the analysis window.
    """
    base = vn_base_symbol(ticker)
    if not end_date:
        end_date = datetime.now(_VN_TZ).strftime("%Y-%m-%d")
    if not start_date:
        start_date = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")

    sections: list[str] = [
        f"Vietnam Market Sentiment & Order-Flow Stream for {base}.VN ({start_date}..{end_date})"
    ]

    if not is_historical(end_date):
        ssi = fetch_ssi_stock_snapshot(ticker)
        if ssi:
            bu_vol = float(ssi.get("stockBUVol") or 0)
            sd_vol = float(ssi.get("stockSDVol") or 0)
            total_active = bu_vol + sd_vol
            bu_pct = round(100 * bu_vol / total_active) if total_active > 0 else 0
            sd_pct = (100 - bu_pct) if total_active > 0 else 0

            buy_f = int(ssi.get("buyForeignQtty") or 0)
            sell_f = int(ssi.get("sellForeignQtty") or 0)
            net_f = buy_f - sell_f
            net_f_val = (float(ssi.get("buyForeignValue") or 0) - float(ssi.get("sellForeignValue") or 0)) / 1e9
            f_tag = "Bullish (Mua ròng)" if net_f > 0 else ("Bearish (Bán ròng)" if net_f < 0 else "Neutral")

            sections.append(
                f"Active Order-Flow Split (Mua/Bán chủ động): "
                f"Bullish/Active Buy (BU): {int(bu_vol):,} shares ({bu_pct}%) · "
                f"Bearish/Active Sell (SD): {int(sd_vol):,} shares ({sd_pct}%)\n"
                f"Foreign Institutional Flow (Khối ngoại): {f_tag} · "
                f"Buy: {buy_f:,} · Sell: {sell_f:,} · Net: {net_f:+,} shares ({net_f_val:+.2f} tỷ VND)"
            )

    articles = get_vn_news(ticker, start_date, end_date, limit=limit)
    if articles:
        lines = ["Recent Vietnamese Financial Media & Investor Forum Headlines:"]
        for art in articles[:limit]:
            pub_str = art["pub_date"].strftime("%Y-%m-%d") if art.get("pub_date") else "?"
            lines.append(f"[{pub_str} · {art['publisher']}] {art['title']}")
        sections.append("\n".join(lines))
    else:
        sections.append(f"<no Vietnamese media/forum items found for {base}.VN within {start_date}..{end_date}>")

    return "\n\n".join(sections)
