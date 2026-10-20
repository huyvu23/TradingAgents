"""DNSE MCP (https://mcp.dnse.com.vn/mcp) OAuth 2.0 + PKCE client for TradingAgents.

Implements:
1. RFC 7591 Dynamic Client Registration against ``https://mcp.dnse.com.vn/oauth2/register``.
2. OAuth 2.0 Authorization Code flow with PKCE (S256) and a local callback
   listener at ``http://127.0.0.1:8765/callback`` that opens the Entrade X
   consent screen in the user's browser.
3. Automatic token persistence and ``refresh_token`` rotation in
   ``~/.tradingagents/dnse_mcp_auth.json``.
4. MCP Streamable HTTP JSON-RPC 2.0 client (supporting both ``application/json``
   and ``text/event-stream`` responses and ``Mcp-Session-Id`` session headers)
   to list and invoke all 38 DNSE MCP tools (Market, Sector, Company, Financial,
   Price/Derivatives, Strategy Backtesting, and Margin Packages).
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import secrets
import subprocess
import sys
import threading
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

DNSE_MCP_ISSUER = "https://mcp.dnse.com.vn"
DNSE_MCP_REGISTER_URL = f"{DNSE_MCP_ISSUER}/oauth2/register"
DNSE_MCP_AUTHORIZE_URL = f"{DNSE_MCP_ISSUER}/oauth2/authorize"
DNSE_MCP_TOKEN_URL = f"{DNSE_MCP_ISSUER}/oauth2/token"
DNSE_MCP_ENDPOINT = f"{DNSE_MCP_ISSUER}/mcp"

CALLBACK_HOST = "127.0.0.1"
CALLBACK_PORT = 8765
REDIRECT_URI = f"http://{CALLBACK_HOST}:{CALLBACK_PORT}/callback"

AUTH_FILE = Path.home() / ".tradingagents" / "dnse_mcp_auth.json"
_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) TradingAgents-DNSE-MCP/1.0"


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _load_auth_state() -> dict[str, Any]:
    if not AUTH_FILE.exists():
        return {}
    try:
        return json.loads(AUTH_FILE.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return {}


def _save_auth_state(state: dict[str, Any]) -> None:
    AUTH_FILE.parent.mkdir(parents=True, exist_ok=True)
    AUTH_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")
    try:
        os.chmod(AUTH_FILE, 0o600)
    except OSError:
        pass


def ensure_client_registered(state: dict[str, Any] | None = None) -> dict[str, Any]:
    """Register an OAuth 2.0 public client with DNSE MCP if not already registered."""
    st = state if state is not None else _load_auth_state()
    if st.get("client_id") and st.get("redirect_uri") == REDIRECT_URI:
        return st

    payload = {
        "client_name": "TradingAgents Vietnam MCP Client",
        "redirect_uris": [REDIRECT_URI],
        "grant_types": ["authorization_code", "refresh_token"],
        "response_types": ["code"],
        "token_endpoint_auth_method": "none",
    }
    req = Request(
        DNSE_MCP_REGISTER_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "User-Agent": _UA},
        method="POST",
    )
    with urlopen(req, timeout=10) as resp:
        reg = json.loads(resp.read().decode("utf-8"))

    st["client_id"] = reg["client_id"]
    st["redirect_uri"] = REDIRECT_URI
    st["client_registered_at"] = int(time.time())
    _save_auth_state(st)
    return st


def refresh_access_token(state: dict[str, Any] | None = None) -> dict[str, Any] | None:
    """Refresh the DNSE MCP OAuth 2.0 access token using the stored refresh_token."""
    st = state if state is not None else _load_auth_state()
    refresh_tok = st.get("refresh_token")
    client_id = st.get("client_id")
    if not refresh_tok or not client_id:
        return None

    form = urlencode(
        {
            "grant_type": "refresh_token",
            "refresh_token": refresh_tok,
            "client_id": client_id,
            "resource": DNSE_MCP_ENDPOINT,
        }
    ).encode("utf-8")
    req = Request(
        DNSE_MCP_TOKEN_URL,
        data=form,
        headers={"Content-Type": "application/x-www-form-urlencoded", "User-Agent": _UA},
        method="POST",
    )
    try:
        with urlopen(req, timeout=10) as resp:
            tok = json.loads(resp.read().decode("utf-8"))
        st["access_token"] = tok["access_token"]
        if tok.get("refresh_token"):
            st["refresh_token"] = tok["refresh_token"]
        expires_in = int(tok.get("expires_in") or 3600)
        st["expires_at"] = int(time.time()) + expires_in
        st.pop("mcp_session_id", None)
        _save_auth_state(st)
        return st
    except Exception as exc:  # noqa: BLE001
        logger.debug("DNSE MCP token refresh failed: %s", exc)
        return None


def get_valid_access_token() -> str | None:
    """Return a valid Bearer access token if available (refreshing if expired)."""
    st = _load_auth_state()
    tok = st.get("access_token")
    if not tok:
        return None
    expires_at = int(st.get("expires_at") or 0)
    if expires_at and time.time() >= (expires_at - 60):
        refreshed = refresh_access_token(st)
        if refreshed and refreshed.get("access_token"):
            return refreshed["access_token"]
        return None
    return tok


def login_dnse_mcp_oauth(timeout_sec: int = 600, open_browser: bool = True) -> dict[str, Any]:
    """Run the interactive OAuth 2.0 PKCE flow with Entrade X in the user's browser."""
    st = ensure_client_registered()
    client_id = st["client_id"]

    code_verifier = _b64url(secrets.token_bytes(32))
    code_challenge = _b64url(hashlib.sha256(code_verifier.encode("ascii")).digest())
    oauth_state = _b64url(secrets.token_bytes(16))

    params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": REDIRECT_URI,
        "scope": "openid",
        "state": oauth_state,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
        "resource": DNSE_MCP_ENDPOINT,
    }
    auth_url = f"{DNSE_MCP_AUTHORIZE_URL}?{urlencode(params)}"

    result_box: dict[str, Any] = {}
    done_event = threading.Event()

    class _CallbackHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
            return

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path != "/callback":
                self.send_response(404)
                self.end_headers()
                return
            qs = parse_qs(parsed.query)
            err = (qs.get("error") or [None])[0]
            code = (qs.get("code") or [None])[0]
            ret_state = (qs.get("state") or [None])[0]

            if err:
                result_box["error"] = err
                result_box["error_description"] = (qs.get("error_description") or [""])[0]
                html_body = (
                    "<html><body style='font-family:sans-serif;text-align:center;padding:40px;'>"
                    "<h2>Kết nối DNSE MCP thất bại</h2>"
                    f"<p>{err}: {result_box['error_description']}</p></body></html>"
                )
            elif not code or ret_state != oauth_state:
                result_box["error"] = "invalid_callback"
                html_body = (
                    "<html><body style='font-family:sans-serif;text-align:center;padding:40px;'>"
                    "<h2>Tham số xác thực không hợp lệ</h2></body></html>"
                )
            else:
                result_box["code"] = code
                html_body = (
                    "<html><head><meta charset='utf-8'><title>DNSE MCP Connected</title></head>"
                    "<body style='font-family:sans-serif;text-align:center;padding:50px;background:#0f172a;color:#f8fafc;'>"
                    "<h1 style='color:#22c55e;'>✅ Đã kết nối thành công DNSE MCP (Entrade X)!</h1>"
                    "<p>Bạn có thể đóng tab trình duyệt này và quay lại cửa sổ chat Antigravity / TradingAgents.</p>"
                    "</body></html>"
                )

            body_bytes = html_body.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body_bytes)))
            self.end_headers()
            self.wfile.write(body_bytes)
            done_event.set()

    HTTPServer.allow_reuse_address = True
    server = HTTPServer((CALLBACK_HOST, CALLBACK_PORT), _CallbackHandler)
    srv_thread = threading.Thread(target=server.serve_forever, daemon=True)
    srv_thread.start()

    print(f"\n[DNSE MCP OAuth 2.0] Mở trình duyệt xác thực Entrade X:\n{auth_url}\n", flush=True)
    if open_browser:
        try:
            if sys.platform == "darwin":
                subprocess.Popen(["open", auth_url])
            else:
                webbrowser.open(auth_url)
        except Exception:  # noqa: BLE001
            pass

    try:
        if not done_event.wait(timeout=timeout_sec):
            raise TimeoutError(f"Quá thời gian chờ đăng nhập Entrade X ({timeout_sec}s).")
    finally:
        server.shutdown()
        server.server_close()

    if "error" in result_box:
        raise RuntimeError(f"DNSE OAuth error: {result_box.get('error')} - {result_box.get('error_description', '')}")

    code = result_box["code"]
    token_form = urlencode(
        {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": REDIRECT_URI,
            "client_id": client_id,
            "code_verifier": code_verifier,
            "resource": DNSE_MCP_ENDPOINT,
        }
    ).encode("utf-8")
    req = Request(
        DNSE_MCP_TOKEN_URL,
        data=token_form,
        headers={"Content-Type": "application/x-www-form-urlencoded", "User-Agent": _UA},
        method="POST",
    )
    with urlopen(req, timeout=15) as resp:
        tok_data = json.loads(resp.read().decode("utf-8"))

    st["access_token"] = tok_data["access_token"]
    if tok_data.get("refresh_token"):
        st["refresh_token"] = tok_data["refresh_token"]
    expires_in = int(tok_data.get("expires_in") or 3600)
    st["expires_at"] = int(time.time()) + expires_in
    st["connected_at"] = int(time.time())
    st.pop("mcp_session_id", None)
    _save_auth_state(st)
    return st


def _parse_mcp_response(raw_bytes: bytes, content_type: str) -> Any:
    text = raw_bytes.decode("utf-8", errors="replace").strip()
    if not text:
        return {}
    if "text/event-stream" in content_type.lower() or text.startswith("event:") or text.startswith("data:"):
        last_json = None
        for line in text.splitlines():
            line = line.strip()
            if line.startswith("data:"):
                data_str = line[5:].strip()
                if data_str:
                    try:
                        last_json = json.loads(data_str)
                    except json.JSONDecodeError:
                        pass
        if last_json is not None:
            return last_json
    return json.loads(text)


def _mcp_rpc(method: str, params: dict[str, Any] | None = None, timeout: float = 20.0) -> Any:
    token = get_valid_access_token()
    if not token:
        raise RuntimeError("Chưa đăng nhập DNSE MCP OAuth 2.0 hoặc phiên đã hết hạn.")

    st = _load_auth_state()
    session_id = st.get("mcp_session_id")

    if not session_id and method != "initialize":
        # Perform MCP initialize handshake first
        init_res, new_sid = _mcp_raw_post(
            token,
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "TradingAgents-VN", "version": "1.0.0"},
                },
            },
            session_id=None,
            timeout=timeout,
        )
        if new_sid:
            session_id = new_sid
            st["mcp_session_id"] = session_id
            _save_auth_state(st)
        # Send initialized notification
        try:
            _mcp_raw_post(
                token,
                {"jsonrpc": "2.0", "method": "notifications/initialized"},
                session_id=session_id,
                timeout=5.0,
            )
        except Exception:  # noqa: BLE001
            pass

    payload: dict[str, Any] = {"jsonrpc": "2.0", "id": int(time.time() * 1000) % 100000, "method": method}
    if params is not None:
        payload["params"] = params

    try:
        res, new_sid = _mcp_raw_post(token, payload, session_id=session_id, timeout=timeout)
    except HTTPError as exc:
        if exc.code in (401, 403):
            refreshed = refresh_access_token(st)
            if refreshed and refreshed.get("access_token"):
                res, new_sid = _mcp_raw_post(refreshed["access_token"], payload, session_id=None, timeout=timeout)
            else:
                raise
        elif exc.code == 404 and session_id:
            # Session expired on server; re-initialize
            st.pop("mcp_session_id", None)
            _save_auth_state(st)
            return _mcp_rpc(method, params=params, timeout=timeout)
        else:
            raise

    if new_sid and new_sid != session_id:
        st["mcp_session_id"] = new_sid
        _save_auth_state(st)
    return res


def _mcp_raw_post(
    token: str,
    payload: dict[str, Any],
    session_id: str | None = None,
    timeout: float = 20.0,
) -> tuple[Any, str | None]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "User-Agent": _UA,
    }
    if session_id:
        headers["Mcp-Session-Id"] = session_id

    req = Request(
        DNSE_MCP_ENDPOINT,
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    with urlopen(req, timeout=timeout) as resp:
        new_sid = resp.headers.get("Mcp-Session-Id") or resp.headers.get("mcp-session-id")
        ctype = resp.headers.get("Content-Type") or ""
        raw = resp.read()
        parsed = _parse_mcp_response(raw, ctype)
        return parsed, new_sid


def list_dnse_mcp_tools() -> list[dict[str, Any]]:
    """List all available tools from the authenticated DNSE MCP server."""
    res = _mcp_rpc("tools/list", {})
    if isinstance(res, dict):
        return (res.get("result") or {}).get("tools") or []
    return []


def call_dnse_mcp_tool(name: str, arguments: dict[str, Any] | None = None, timeout: float = 25.0) -> Any:
    """Invoke a specific tool on the DNSE MCP server and return its extracted content."""
    res = _mcp_rpc("tools/call", {"name": name, "arguments": arguments or {}}, timeout=timeout)
    if not isinstance(res, dict):
        return res
    if "error" in res:
        raise RuntimeError(f"DNSE MCP tool error ({name}): {res['error']}")
    result = res.get("result") or {}
    contents = result.get("content")
    if isinstance(contents, list):
        texts = []
        for item in contents:
            if isinstance(item, dict) and item.get("type") == "text":
                texts.append(item.get("text", ""))
        joined = "\n".join(texts).strip()
        try:
            return json.loads(joined)
        except Exception:  # noqa: BLE001
            return joined
    return result


def fetch_dnse_mcp_enrichment(ticker: str) -> str:
    """If authenticated with DNSE MCP OAuth 2.0, fetch official Entrade X / DNSE data for ``ticker``."""
    if not get_valid_access_token():
        return ""
    base = ticker.strip().upper().removesuffix(".VN")
    lines: list[str] = [f"### Dữ liệu Chuyên sâu từ DNSE MCP / Entrade X ({base}.VN)"]

    try:
        idx_res = call_dnse_mcp_tool("financial_get_index", {"symbol": base}, timeout=8.0)
        indexes = ((idx_res or {}).get("data") or {}).get("indexes") or {}
        if indexes:
            kv_pairs: list[str] = []
            for key, label, fmt in (
                ("debtEquityRatio", "Nợ/VCSH (D/E)", lambda v: f"{float(v):.2f}x"),
                ("grossMargin", "Biên lợi nhuận gộp", lambda v: f"{float(v) * 100:.2f}%"),
                ("inventoryGrowth", "Tăng trưởng Tồn kho", lambda v: f"{float(v) * 100:+.2f}%"),
                ("prepaidBuyerGrowth", "Người mua trả tiền trước", lambda v: f"{float(v) * 100:+.2f}%"),
                ("profitGrowth", "Tăng trưởng Lợi nhuận", lambda v: f"{float(v) * 100:+.1f}%"),
                ("bookValue", "Giá trị sổ sách (BVPS - DNSE)", lambda v: f"{float(v):,.0f} VND"),
                ("foreignHoldingRatio", "Tỷ lệ sở hữu Khối ngoại", lambda v: f"{float(v):.2f}%"),
            ):
                entry = indexes.get(key)
                if isinstance(entry, dict) and entry.get("value") is not None:
                    try:
                        kv_pairs.append(f"**{label}**: {fmt(entry['value'])}")
                    except Exception:  # noqa: BLE001
                        kv_pairs.append(f"**{label}**: {entry['value']}")
            if kv_pairs:
                lines.append("- **Chỉ số Tài chính Chuyên sâu (DNSE)**: " + " | ".join(kv_pairs))
    except Exception as exc:  # noqa: BLE001
        logger.debug("DNSE MCP financial_get_index failed for %s: %s", base, exc)

    try:
        mkt_res = call_dnse_mcp_tool("market_get_overview", {}, timeout=8.0)
        m_data = (mkt_res or {}).get("data") or {}
        ment = m_data.get("mentalities") or {}
        if ment.get("currentSession") is not None:
            lines.append(
                f"- **Chỉ số Tâm lý Thị trường DNSE (Market Mentality 0-100)**: "
                f"Phiên hiện tại: **{ment.get('currentSession')}/100** "
                f"(Phiên trước: {ment.get('previousSession')}/100 | "
                f"5 phiên trước: {ment.get('fiveSessionsAgo')}/100 | "
                f"20 phiên trước: {ment.get('twentySessionsAgo')}/100)"
            )
    except Exception as exc:  # noqa: BLE001
        logger.debug("DNSE MCP market_get_overview failed: %s", exc)

    try:
        ev_res = call_dnse_mcp_tool("company_get_company_events", {"symbol": base}, timeout=8.0)
        actions = (((ev_res or {}).get("data") or {}).get("corporate_actions_history") or {}).get("corporateActions") or []
        if actions:
            recent_ev = [
                f"{a.get('exRightsDate', '')[:10]}: {a.get('title') or a.get('content')}"
                for a in actions[:3]
                if a.get("title") or a.get("content")
            ]
            if recent_ev:
                lines.append("- **Lịch sử Sự kiện & Cổ tức gần nhất (DNSE)**: " + " ; ".join(recent_ev))
    except Exception as exc:  # noqa: BLE001
        logger.debug("DNSE MCP company_get_company_events failed for %s: %s", base, exc)

    return "\n".join(lines) if len(lines) > 1 else ""


# ---------------------------------------------------------------------------
# DNSE LightSpeed Trading API (https://services.entrade.com.vn)
# Used for reading personal sub-accounts, real cash balance, order history,
# and held deals/positions.
# ---------------------------------------------------------------------------

DNSE_ENTRADE_BASE = "https://services.entrade.com.vn"


def get_lightspeed_token(force_refresh: bool = False) -> str:
    """Authenticate against DNSE LightSpeed API using DNSE_USERNAME & DNSE_PASSWORD from .env."""
    st = _load_auth_state()
    if not force_refresh:
        cached = st.get("lightspeed_token")
        exp = int(st.get("lightspeed_expires_at") or 0)
        if cached and time.time() < (exp - 120):
            return str(cached)

    try:
        from dotenv import load_dotenv

        repo_root_env = Path(__file__).resolve().parents[3] / ".env"
        load_dotenv(repo_root_env)
        load_dotenv(Path.home() / ".env")
    except Exception:  # noqa: BLE001
        pass

    username = (os.environ.get("DNSE_USERNAME") or "").strip()
    password = (os.environ.get("DNSE_PASSWORD") or "").strip()
    if not username or not password:
        raise RuntimeError("Thiếu DNSE_USERNAME hoặc DNSE_PASSWORD trong file .env.")

    req = Request(
        f"{DNSE_ENTRADE_BASE}/dnse-auth-service/login",
        data=json.dumps({"username": username, "password": password}).encode("utf-8"),
        headers={"Content-Type": "application/json", "User-Agent": _UA},
        method="POST",
    )
    with urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    tok = data.get("token")
    if not tok:
        raise RuntimeError(f"Đăng nhập DNSE LightSpeed thất bại: {data}")

    st["lightspeed_token"] = tok
    # LightSpeed JWT token is valid for 8 hours (28800 seconds)
    st["lightspeed_expires_at"] = int(time.time()) + 28000
    _save_auth_state(st)
    return str(tok)


def _lightspeed_get(path: str, timeout: float = 15.0) -> Any:
    tok = get_lightspeed_token()
    req = Request(
        f"{DNSE_ENTRADE_BASE}{path}",
        headers={"Authorization": f"Bearer {tok}", "User-Agent": _UA},
        method="GET",
    )
    try:
        with urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except HTTPError as exc:
        if exc.code in (401, 403):
            tok = get_lightspeed_token(force_refresh=True)
            req2 = Request(
                f"{DNSE_ENTRADE_BASE}{path}",
                headers={"Authorization": f"Bearer {tok}", "User-Agent": _UA},
                method="GET",
            )
            with urlopen(req2, timeout=timeout) as resp2:
                return json.loads(resp2.read().decode("utf-8"))
        raise


def dnse_get_accounts() -> list[dict[str, Any]]:
    """Return list of sub-accounts (tiểu khoản) on Entrade X."""
    res = _lightspeed_get("/dnse-order-service/accounts")
    if isinstance(res, dict):
        return res.get("accounts") or res.get("default") or []
    return res if isinstance(res, list) else []


def dnse_get_account_balance(account_no: str) -> dict[str, Any]:
    """Return cash balance and buying power for a specific sub-account."""
    return _lightspeed_get(f"/dnse-order-service/account-balances/{account_no}")


def dnse_get_orders(account_no: str) -> list[dict[str, Any]]:
    """Return order book (sổ lệnh mua/bán) for a specific sub-account."""
    res = _lightspeed_get(f"/dnse-order-service/v2/orders?accountNo={account_no}")
    if isinstance(res, dict):
        return res.get("orders") or []
    return res if isinstance(res, list) else []


def dnse_get_deals(account_no: str) -> list[dict[str, Any]]:
    """Return held deals/positions (danh sách deal nắm giữ) for a specific sub-account."""
    res = _lightspeed_get(f"/dnse-deal-service/deals?accountNo={account_no}")
    if isinstance(res, dict):
        return res.get("deals") or []
    return res if isinstance(res, list) else []


if __name__ == "__main__":
    st = login_dnse_mcp_oauth(timeout_sec=600, open_browser=True)
    print("✅ Đăng nhập OAuth 2.0 DNSE MCP thành công! Đang kiểm tra danh sách công cụ...", flush=True)
    tools = list_dnse_mcp_tools()
    print(f"✅ Đã tải thành công {len(tools)} công cụ từ DNSE MCP!", flush=True)
    for t in tools[:15]:
        print(f"  - {t.get('name')}: {t.get('description', '')[:80]}", flush=True)
