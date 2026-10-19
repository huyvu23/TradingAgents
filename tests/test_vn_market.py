"""Unit tests for Vietnam Stock Market (.VN) enhancements."""

from datetime import UTC, datetime
from unittest.mock import patch

import pandas as pd
import pytest

from tradingagents.agents.context import _identity, build_instrument_context, resolve_instrument_identity
from tradingagents.dataflows.vendors import stocktwits, vn_market
from tradingagents.dataflows.vendors.yahoo import fundamentals as yfund
from tradingagents.dataflows.vendors.yahoo import market as ymarket
from tradingagents.dataflows.vendors.yahoo import news as ynews
from tradingagents.default_config import DEFAULT_CONFIG


@pytest.mark.unit
def test_vn_benchmark_mapped_in_default_config():
    assert DEFAULT_CONFIG["benchmark_map"].get(".VN") == "E1VFVN30.VN"


@pytest.mark.unit
def test_is_vn_ticker_and_base_symbol():
    assert vn_market.is_vn_ticker("MSN.VN") is True
    assert vn_market.is_vn_ticker("pvs.vn") is True
    assert vn_market.is_vn_ticker("AAPL") is False
    assert vn_market.vn_base_symbol("msn.vn") == "MSN"


@pytest.mark.unit
def test_build_instrument_context_injects_vn_trading_rules():
    ctx = build_instrument_context(
        "MSN.VN",
        "stock",
        {"company_name": "Masan Group Corporation", "exchange": "HOSE"},
        "2026-10-19",
    )
    assert "T+2.5" in ctx
    assert "HOSE (±7% daily limit)" in ctx
    assert "100 shares/order" in ctx
    assert "Foreign Investor Net Flow" in ctx


@pytest.mark.unit
def test_resolve_identity_falls_back_to_vn_profile_for_hnx_upcom():
    _identity.cache_clear()
    with (
        patch("tradingagents.agents.context.get_company_profile", return_value={}),
        patch(
            "tradingagents.agents.context.get_vn_company_profile",
            return_value={
                "longName": "Tổng Công ty cổ phần Dịch vụ Kỹ thuật Dầu khí Việt Nam",
                "sector": "Năng lượng",
                "industry": "Dịch vụ và Thiết bị Dầu khí",
                "exchange": "HNX",
            },
        ),
    ):
        ident = resolve_instrument_identity("PVS.VN")
    assert ident["company_name"] == "Tổng Công ty cổ phần Dịch vụ Kỹ thuật Dầu khí Việt Nam"
    assert ident["exchange"] == "HNX"
    assert ident["sector"] == "Năng lượng"


@pytest.mark.unit
def test_get_news_yfinance_uses_vn_news_for_vn_tickers(monkeypatch):
    monkeypatch.setattr(
        ynews,
        "get_vn_news",
        lambda ticker, start, end, limit=20: [
            {
                "title": "Masan tăng sở hữu tại MCH",
                "summary": "Tin tức doanh nghiệp",
                "publisher": "VnExpress",
                "link": "https://vnexpress.net/msn",
                "pub_date": datetime(2026, 10, 15, tzinfo=UTC),
            }
        ],
    )
    monkeypatch.setattr(
        ynews,
        "get_vn_global_news_articles",
        lambda as_of, look_back_days=7, limit=6: [
            {
                "title": "VN-Index vượt mốc cản tâm lý",
                "summary": "",
                "publisher": "CafeF",
                "link": "https://cafef.vn/vnindex",
                "pub_date": datetime(2026, 10, 16, tzinfo=UTC),
            }
        ],
    )
    out = ynews.get_news_yfinance("MSN.VN", "2026-10-10", "2026-10-19")
    assert "Masan tăng sở hữu tại MCH" in out
    assert "VN-Index vượt mốc cản tâm lý" in out


@pytest.mark.unit
def test_stocktwits_routes_vn_tickers_to_vn_sentiment(monkeypatch):
    monkeypatch.setattr(
        stocktwits,
        "fetch_vn_market_sentiment",
        lambda ticker, limit=30, start_date=None, end_date=None: (
            f"VN_SENTIMENT:{ticker}:{start_date}..{end_date}"
        ),
    )
    out = stocktwits.fetch_stocktwits_messages(
        "MSN.VN", limit=15, start_date="2026-10-12", end_date="2026-10-19"
    )
    assert out == "VN_SENTIMENT:MSN.VN:2026-10-12..2026-10-19"


@pytest.mark.unit
def test_fundamentals_uses_vn_enrichment_when_yfinance_empty(monkeypatch):
    monkeypatch.setattr(yfund, "yf_retry", lambda fn: {})
    monkeypatch.setattr(
        yfund,
        "get_vn_fundamentals_enrichment",
        lambda ticker, as_of_date=None: "## Vietnam Market Local Data & Order Flow (PVS.VN)\n- **Sàn niêm yết**: HNX",
    )
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    out = yfund.get_fundamentals("PVS.VN", today)
    assert "Company Fundamentals for PVS.VN" in out
    assert "Sàn niêm yết" in out


@pytest.mark.unit
def test_market_falls_back_to_vn_ohlcv_when_yfinance_empty(monkeypatch):
    monkeypatch.setattr(ymarket, "yf_retry", lambda fn: pd.DataFrame())
    fake_df = pd.DataFrame(
        {
            "Date": pd.to_datetime(["2026-10-15", "2026-10-16"]),
            "Open": [31000.0, 31400.0],
            "High": [31800.0, 32200.0],
            "Low": [30900.0, 31200.0],
            "Close": [31400.0, 32000.0],
            "Adj Close": [31400.0, 32000.0],
            "Volume": [2500000, 3100000],
        }
    )
    monkeypatch.setattr(ymarket, "get_vn_ohlcv_df", lambda sym, s, e: fake_df)
    out = ymarket.get_YFin_data_online("PVS.VN", "2026-10-10", "2026-10-19")
    assert "Stock data for PVS.VN" in out
    assert "32000.0" in out
