"""
Bot de trading sobre Alpaca — ACCIONES y CRIPTO, 2 estrategias.

ESTRATEGIAS (STRATEGY en .env):
  - "fixed"    : compra si precio <= BUY_PRICE; vende si >= SELL_PRICE.
  - "trailing" : detecta valles y picos por porcentaje:
        * ESPERANDO_COMPRA: sigue el precio hacia abajo memorizando el MÍNIMO.
          Compra cuando el precio REBOTA >= REBOUND_PCT % desde ese mínimo.
        * ESPERANDO_VENTA: sigue el precio hacia arriba memorizando el MÁXIMO.
          Vende cuando el precio CAE >= DROP_PCT % desde ese máximo.

Símbolo (SYMBOL en .env):
  - Acciones: "AMD", "AAPL"  → opera solo con mercado abierto (L-V).
  - Cripto:   "BTC/USD", ... → opera 24/7. Se detecta por la "/".

Seguridad:
  - DRY_RUN=true por defecto: simula órdenes, NUNCA llama al endpoint de órdenes.
  - Credenciales solo en .env (o Secretos de Colab si corres ahí).

Uso local:
    pip install alpaca-py python-dotenv
    python bot.py

Uso en Colab (opcional):
    Celda 1:  !pip install alpaca-py python-dotenv -q
    Celda 2:  (pegar este archivo)
    Celda 3:  setup_logging(); Bot().run()

Todo queda registrado en SQLite (data/bot.db) y en logs/bot.log.
"""

import logging
import logging.handlers
import math
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from alpaca.common.exceptions import APIError
from alpaca.data.enums import DataFeed
from alpaca.data.historical import CryptoHistoricalDataClient, StockHistoricalDataClient
from alpaca.data.requests import CryptoLatestQuoteRequest, StockLatestTradeRequest
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.trading.requests import LimitOrderRequest

# ============================================================
# Configuración (.env primero; en Colab también lee Secretos)
# ============================================================
IN_COLAB = False
try:
    from google.colab import userdata  # noqa: F401
    IN_COLAB = True
except ImportError:
    pass

if IN_COLAB:
    ROOT = Path("/content")
else:
    ROOT = Path(__file__).resolve().parent

# Siempre intentar cargar .env (local y también si se sube a Colab)
try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass


def _env(nombre, default=""):
    """Lee variable de entorno / .env; en Colab también prueba Secretos."""
    if IN_COLAB:
        try:
            valor = userdata.get(nombre)
            if valor is not None and str(valor).strip() != "":
                return str(valor).strip()
        except Exception:
            pass
    return os.getenv(nombre, default)


API_KEY = _env("APCA_API_KEY_ID")
SECRET_KEY = _env("APCA_API_SECRET_KEY")
BASE_URL = _env("APCA_API_BASE_URL", "https://paper-api.alpaca.markets").rstrip("/")
if BASE_URL.endswith("/v2"):
    BASE_URL = BASE_URL[:-3]
IS_PAPER = "paper" in BASE_URL.lower()

SYMBOL = _env("SYMBOL", "BTC/USD").upper()
IS_CRYPTO = "/" in SYMBOL

STRATEGY = _env("STRATEGY", "trailing").lower()  # fixed | trailing

# --- Estrategia "fixed" ---
BUY_PRICE = float(_env("BUY_PRICE", "64300"))
SELL_PRICE = float(_env("SELL_PRICE", "64350"))

# --- Estrategia "trailing" (porcentajes) ---
REBOUND_PCT = float(_env("REBOUND_PCT", "0.15"))  # rebote desde mínimo
DROP_PCT = float(_env("DROP_PCT", "0.15"))        # caída desde máximo

MIN_NOTIONAL = 1.00
DRY_RUN = _env("DRY_RUN", "true").strip().lower() in ("1", "true", "yes", "on")
POLL_SECONDS = int(_env("POLL_INTERVAL_SECONDS", "60"))
CLOSED_MARKET_SLEEP = 300

DB_PATH = ROOT / "data" / "bot.db"
LOG_DIR = ROOT / "logs"

ESPERANDO_COMPRA = "ESPERANDO_COMPRA"
ESPERANDO_VENTA = "ESPERANDO_VENTA"
DEAD_ORDER_STATUSES = {"canceled", "expired", "rejected", "done_for_day", "replaced"}

log = logging.getLogger("bot")


def setup_logging():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s",
                            "%Y-%m-%d %H:%M:%S")
    file_h = logging.handlers.RotatingFileHandler(
        LOG_DIR / "bot.log", maxBytes=5 * 1024 * 1024, backupCount=7,
        encoding="utf-8")
    file_h.setFormatter(fmt)
    console_h = logging.StreamHandler(sys.stdout)
    console_h.setFormatter(fmt)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()
    root.addHandler(file_h)
    root.addHandler(console_h)


def now_utc():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ============================================================
# Persistencia SQLite
# ============================================================
def open_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS price_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT NOT NULL,
        symbol TEXT NOT NULL, price REAL NOT NULL, source TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS state_transitions (
        id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT NOT NULL,
        from_state TEXT NOT NULL, to_state TEXT NOT NULL,
        trigger_price REAL NOT NULL);
    CREATE TABLE IF NOT EXISTS orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT NOT NULL,
        side TEXT NOT NULL, qty REAL, notional REAL, limit_price REAL NOT NULL,
        alpaca_order_id TEXT, status TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS bot_state (
        id INTEGER PRIMARY KEY CHECK (id = 1), state TEXT NOT NULL,
        symbol TEXT NOT NULL DEFAULT '',
        position_qty REAL NOT NULL DEFAULT 0,
        capital_available REAL NOT NULL DEFAULT 0,
        pending_order_id TEXT,
        watermark REAL,
        avg_entry REAL,
        updated_at TEXT NOT NULL);
    """)
    conn.commit()
    # Migración suave para DBs de versiones anteriores
    cols = [r["name"] for r in conn.execute("PRAGMA table_info(bot_state)")]
    for col, ddl in (("symbol", "TEXT NOT NULL DEFAULT ''"),
                     ("watermark", "REAL"),
                     ("avg_entry", "REAL")):
        if col not in cols:
            conn.execute(f"ALTER TABLE bot_state ADD COLUMN {col} {ddl}")
    conn.commit()
    return conn


class Bot:
    def __init__(self):
        if not API_KEY or not SECRET_KEY:
            log.error("Faltan APCA_API_KEY_ID / APCA_API_SECRET_KEY en .env")
            sys.exit(1)
        if STRATEGY not in ("fixed", "trailing"):
            log.error("STRATEGY debe ser 'fixed' o 'trailing' (actual: %s)",
                      STRATEGY)
            sys.exit(1)
        if STRATEGY == "fixed" and SELL_PRICE <= BUY_PRICE:
            log.error("SELL_PRICE (%.2f) debe ser mayor que BUY_PRICE (%.2f)",
                      SELL_PRICE, BUY_PRICE)
            sys.exit(1)
        if STRATEGY == "trailing" and (REBOUND_PCT <= 0 or DROP_PCT <= 0):
            log.error("REBOUND_PCT y DROP_PCT deben ser > 0")
            sys.exit(1)

        self.trading = TradingClient(API_KEY, SECRET_KEY, paper=IS_PAPER)
        if IS_CRYPTO:
            self.data = CryptoHistoricalDataClient(API_KEY, SECRET_KEY)
        else:
            self.data = StockHistoricalDataClient(API_KEY, SECRET_KEY)
        self.db = open_db()

        self.state = ESPERANDO_COMPRA
        self.position_qty = 0.0
        self.capital = 0.0
        self.pending_order_id = None    # orden real abierta en Alpaca
        self.dry_run_pending = None     # (side, amount, price) simulado
        self.watermark = None           # mínimo (comprando) o máximo (vendiendo)
        self.avg_entry = None           # precio de compra de la posición actual

    # --------------------------------------------------------
    # Helpers de API con reintentos (backoff exponencial)
    # --------------------------------------------------------
    def _retry(self, description, func, *args):
        for attempt in range(1, 5):
            try:
                return func(*args)
            except APIError as exc:
                status = getattr(exc, "status_code", None)
                if status not in (429, 500, 502, 503, 504) or attempt == 4:
                    raise
                delay = 2.0 * (2 ** (attempt - 1))
                log.warning("%s: status %s, reintento %d en %.0fs",
                            description, status, attempt, delay)
                time.sleep(delay)
            except Exception as exc:
                if attempt == 4:
                    raise
                delay = 2.0 * (2 ** (attempt - 1))
                log.warning("%s: error de red (%s), reintento %d en %.0fs",
                            description, exc, attempt, delay)
                time.sleep(delay)

    # --------------------------------------------------------
    # Consultas a Alpaca
    # --------------------------------------------------------
    def verify_asset(self):
        asset = self._retry("get_asset", self.trading.get_asset, SYMBOL)
        if not asset.tradable:
            log.error("%s NO es operable. Abortando.", SYMBOL)
            sys.exit(2)
        if not IS_CRYPTO and not asset.fractionable:
            log.error("%s NO es fraccionable. Abortando.", SYMBOL)
            sys.exit(2)
        log.info("%s verificado. Tipo: %s.",
                 SYMBOL, "CRIPTO (24/7)" if IS_CRYPTO else "ACCIÓN")

    def latest_price(self):
        if IS_CRYPTO:
            # Quote (punto medio bid/ask): se actualiza aunque no haya trades
            req = CryptoLatestQuoteRequest(symbol_or_symbols=SYMBOL)
            quotes = self._retry("latest_quote",
                                 self.data.get_crypto_latest_quote, req)
            q = quotes[SYMBOL]
            return (float(q.bid_price) + float(q.ask_price)) / 2
        req = StockLatestTradeRequest(symbol_or_symbols=SYMBOL,
                                      feed=DataFeed.IEX)
        trades = self._retry("latest_trade",
                             self.data.get_stock_latest_trade, req)
        return float(trades[SYMBOL].price)

    def safe_buying_power(self):
        """Poder de compra conservador para cuenta cash."""
        acct = self._retry("get_account", self.trading.get_account)
        if acct.trading_blocked or acct.account_blocked:
            log.error("Cuenta bloqueada para operar (trading_blocked=%s).",
                      acct.trading_blocked)
            return 0.0
        return min(float(acct.cash), float(acct.buying_power),
                   float(acct.non_marginable_buying_power))

    def real_position_qty(self):
        try:
            pos = self._retry("get_position", self.trading.get_open_position,
                              SYMBOL.replace("/", ""))
            return float(pos.qty)
        except APIError as exc:
            if getattr(exc, "status_code", None) == 404:
                return 0.0
            raise

    def market_is_open(self):
        if IS_CRYPTO:
            return True, None
        clock = self._retry("get_clock", self.trading.get_clock)
        return bool(clock.is_open), clock.next_open

    # --------------------------------------------------------
    # Persistencia de estado
    # --------------------------------------------------------
    def persist(self):
        self.db.execute(
            "INSERT INTO bot_state (id, state, symbol, position_qty, "
            "capital_available, pending_order_id, watermark, avg_entry, "
            "updated_at) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(id) DO UPDATE SET state=excluded.state, "
            "symbol=excluded.symbol, position_qty=excluded.position_qty, "
            "capital_available=excluded.capital_available, "
            "pending_order_id=excluded.pending_order_id, "
            "watermark=excluded.watermark, avg_entry=excluded.avg_entry, "
            "updated_at=excluded.updated_at",
            (self.state, SYMBOL, self.position_qty, self.capital,
             self.pending_order_id, self.watermark, self.avg_entry, now_utc()))
        self.db.commit()

    def log_transition(self, old_state, price):
        self.db.execute(
            "INSERT INTO state_transitions (timestamp, from_state, to_state, "
            "trigger_price) VALUES (?, ?, ?, ?)",
            (now_utc(), old_state, self.state, price))
        self.db.commit()
        log.info("Transición %s -> %s (precio %.2f)", old_state, self.state, price)

    def record_order(self, side, limit_price, status, qty=None, notional=None,
                     alpaca_id=None):
        self.db.execute(
            "INSERT INTO orders (timestamp, side, qty, notional, limit_price, "
            "alpaca_order_id, status) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (now_utc(), side, qty, notional, limit_price, alpaca_id, status))
        self.db.commit()

    # --------------------------------------------------------
    # Arranque
    # --------------------------------------------------------
    def startup(self):
        self.verify_asset()
        row = self.db.execute("SELECT * FROM bot_state WHERE id = 1").fetchone()
        if row is not None and row["symbol"] not in ("", SYMBOL):
            log.warning("La DB guardada era de %s pero ahora el símbolo es %s. "
                        "Se reinicia el estado.", row["symbol"], SYMBOL)
            row = None
        if row is not None:
            self.state = row["state"]
            self.position_qty = row["position_qty"]
            self.capital = row["capital_available"]
            self.pending_order_id = row["pending_order_id"]
            self.watermark = row["watermark"]
            self.avg_entry = row["avg_entry"]
            log.info("Estado restaurado: %s (posición=%.9f, capital=%.2f, "
                     "watermark=%s)", self.state, self.position_qty,
                     self.capital, self.watermark)
        else:
            real_qty = self.real_position_qty()
            if real_qty > 0:
                self.state = ESPERANDO_VENTA
                self.position_qty = real_qty
            self.capital = self.safe_buying_power()
            log.info("Corrida inicial: %s (posición=%.9f, capital=%.2f)",
                     self.state, self.position_qty, self.capital)
            self.persist()

        if not DRY_RUN:
            real_qty = self.real_position_qty()
            if abs(real_qty - self.position_qty) > 1e-9:
                log.warning("Reconciliación: DB=%.9f vs Alpaca=%.9f. Manda Alpaca.",
                            self.position_qty, real_qty)
                self.position_qty = real_qty
                self.state = ESPERANDO_VENTA if real_qty > 0 else ESPERANDO_COMPRA
                self.watermark = None
                self.persist()

        if STRATEGY == "trailing":
            params = "rebote >= %.2f%% desde mínimo | caída >= %.2f%% desde máximo" % (
                REBOUND_PCT, DROP_PCT)
        else:
            params = "compra <= %.2f | venta >= %.2f" % (BUY_PRICE, SELL_PRICE)
        log.info("Bot listo. %s | %s | Colab=%s | Paper=%s | DRY_RUN=%s | %s | "
                 "cada %ds", SYMBOL, STRATEGY.upper(), IN_COLAB, IS_PAPER,
                 DRY_RUN, params, POLL_SECONDS)

    # --------------------------------------------------------
    # Órdenes (con guard de DRY_RUN)
    # --------------------------------------------------------
    def place_buy(self, limit_price):
        if not DRY_RUN:
            self.capital = self.safe_buying_power()
        notional = math.floor(self.capital * 100) / 100
        if notional < MIN_NOTIONAL:
            log.warning("Capital %.2f menor al mínimo de $%.2f. "
                        "Se reintenta en la próxima iteración.",
                        self.capital, MIN_NOTIONAL)
            return

        limit_price = round(limit_price, 2)
        if DRY_RUN:
            log.info("[DRY_RUN] Hubiera enviado: BUY %s notional=$%.2f limit=%.2f",
                     SYMBOL, notional, limit_price)
            self.record_order("buy", limit_price, "dry_run", notional=notional)
            self.dry_run_pending = ("buy", notional, limit_price)
            return

        if IS_CRYPTO:
            qty = math.floor((notional / limit_price) * 0.999 * 1e9) / 1e9
            req = LimitOrderRequest(symbol=SYMBOL, qty=qty, side=OrderSide.BUY,
                                    time_in_force=TimeInForce.GTC,
                                    limit_price=limit_price)
            order = self._retry("submit_buy", self.trading.submit_order, req)
            log.info("Orden BUY enviada: id=%s qty=%.9f limit=%.2f",
                     order.id, qty, limit_price)
            self.record_order("buy", limit_price, str(order.status), qty=qty,
                              alpaca_id=str(order.id))
        else:
            req = LimitOrderRequest(symbol=SYMBOL, notional=notional,
                                    side=OrderSide.BUY,
                                    time_in_force=TimeInForce.DAY,
                                    limit_price=limit_price)
            order = self._retry("submit_buy", self.trading.submit_order, req)
            log.info("Orden BUY enviada: id=%s notional=%.2f limit=%.2f",
                     order.id, notional, limit_price)
            self.record_order("buy", limit_price, str(order.status),
                              notional=notional, alpaca_id=str(order.id))
        self.pending_order_id = str(order.id)
        self.persist()

    def place_sell(self, limit_price):
        qty = math.floor(self.position_qty * 1e9) / 1e9
        if qty <= 0:
            log.error("Señal de venta pero no hay posición (qty=%.9f).",
                      self.position_qty)
            return

        limit_price = round(limit_price, 2)
        if DRY_RUN:
            log.info("[DRY_RUN] Hubiera enviado: SELL %s qty=%.9f limit=%.2f",
                     SYMBOL, qty, limit_price)
            self.record_order("sell", limit_price, "dry_run", qty=qty)
            self.dry_run_pending = ("sell", qty, limit_price)
            return

        tif = TimeInForce.GTC if IS_CRYPTO else TimeInForce.DAY
        req = LimitOrderRequest(symbol=SYMBOL, qty=qty, side=OrderSide.SELL,
                                time_in_force=tif, limit_price=limit_price)
        order = self._retry("submit_sell", self.trading.submit_order, req)
        log.info("Orden SELL enviada: id=%s qty=%.9f limit=%.2f",
                 order.id, qty, limit_price)
        self.record_order("sell", limit_price, str(order.status), qty=qty,
                          alpaca_id=str(order.id))
        self.pending_order_id = str(order.id)
        self.persist()

    # --------------------------------------------------------
    # Manejo de la orden pendiente (fill real o simulado)
    # --------------------------------------------------------
    def check_pending(self):
        if self.dry_run_pending is not None:
            side, amount, price = self.dry_run_pending
            self.dry_run_pending = None
            old = self.state
            if side == "buy":
                self.state = ESPERANDO_VENTA
                self.position_qty = round(amount / price, 9)
                self.capital -= amount
                self.avg_entry = price
            else:
                self.state = ESPERANDO_COMPRA
                self.capital += amount * price
                if self.avg_entry:
                    pnl = (price - self.avg_entry) * amount
                    log.info("[DRY_RUN] Resultado del ciclo: %+.2f USD "
                             "(compra %.2f -> venta %.2f)",
                             pnl, self.avg_entry, price)
                self.position_qty = 0.0
                self.avg_entry = None
            self.watermark = None  # reiniciar seguimiento en el nuevo estado
            log.info("[DRY_RUN] FILL simulado %s a %.2f (posición=%.9f, capital=%.2f)",
                     side.upper(), price, self.position_qty, self.capital)
            self.log_transition(old, price)
            self.persist()
            return

        if self.pending_order_id is None:
            return

        order = self._retry("get_order", self.trading.get_order_by_id,
                            self.pending_order_id)
        status = str(getattr(order.status, "value", order.status)).lower()

        if status == "filled":
            filled_qty = float(order.filled_qty or 0)
            fill_price = float(order.filled_avg_price or 0)
            old = self.state
            if self.state == ESPERANDO_COMPRA:
                self.state = ESPERANDO_VENTA
                self.position_qty = filled_qty
                self.avg_entry = fill_price
            else:
                self.state = ESPERANDO_COMPRA
                if self.avg_entry:
                    pnl = (fill_price - self.avg_entry) * filled_qty
                    log.info("Resultado del ciclo: %+.2f USD (compra %.2f -> "
                             "venta %.2f)", pnl, self.avg_entry, fill_price)
                self.position_qty = 0.0
                self.avg_entry = None
            self.capital = self.safe_buying_power()
            self.watermark = None
            self.db.execute("UPDATE orders SET status='filled' WHERE alpaca_order_id=?",
                            (self.pending_order_id,))
            self.db.commit()
            log.info("FILL: qty=%.9f a %.2f", filled_qty, fill_price)
            self.log_transition(old, fill_price)
            self.pending_order_id = None
            self.persist()
        elif status in DEAD_ORDER_STATUSES:
            log.warning("Orden %s terminó sin llenarse (status=%s). Se re-evalúa.",
                        self.pending_order_id, status)
            self.db.execute("UPDATE orders SET status=? WHERE alpaca_order_id=?",
                            (status, self.pending_order_id))
            self.db.commit()
            self.pending_order_id = None
            self.watermark = None
            self.persist()
        else:
            log.info("Orden %s sigue abierta (status=%s).",
                     self.pending_order_id, status)

    # --------------------------------------------------------
    # Lógica de decisión
    # --------------------------------------------------------
    def decide_fixed(self, price):
        if self.state == ESPERANDO_COMPRA and price <= BUY_PRICE:
            log.info("Umbral de COMPRA alcanzado (%.2f <= %.2f)", price, BUY_PRICE)
            self.place_buy(BUY_PRICE)
        elif self.state == ESPERANDO_VENTA and price >= SELL_PRICE:
            log.info("Umbral de VENTA alcanzado (%.2f >= %.2f)", price, SELL_PRICE)
            self.place_sell(SELL_PRICE)

    def decide_trailing(self, price):
        if self.state == ESPERANDO_COMPRA:
            if self.watermark is None or price < self.watermark:
                if self.watermark is not None:
                    log.info("Nuevo MÍNIMO: %.2f (gatillo de compra: %.2f)",
                             price, price * (1 + REBOUND_PCT / 100))
                self.watermark = price
                self.persist()
                return
            gatillo = self.watermark * (1 + REBOUND_PCT / 100)
            if price >= gatillo:
                log.info("REBOTE desde el valle: mínimo=%.2f, precio=%.2f "
                         "(>= %.2f). Comprando.", self.watermark, price, gatillo)
                self.place_buy(price)
        else:  # ESPERANDO_VENTA
            if self.watermark is None or price > self.watermark:
                if self.watermark is not None:
                    log.info("Nuevo MÁXIMO: %.2f (gatillo de venta: %.2f)",
                             price, price * (1 - DROP_PCT / 100))
                self.watermark = price
                self.persist()
                return
            gatillo = self.watermark * (1 - DROP_PCT / 100)
            if price <= gatillo:
                log.info("CAÍDA desde el pico: máximo=%.2f, precio=%.2f "
                         "(<= %.2f). Vendiendo.", self.watermark, price, gatillo)
                self.place_sell(price)

    # --------------------------------------------------------
    # Una iteración del loop
    # --------------------------------------------------------
    def step(self):
        price = self.latest_price()
        self.db.execute(
            "INSERT INTO price_log (timestamp, symbol, price, source) "
            "VALUES (?, ?, ?, ?)", (now_utc(), SYMBOL, price, "rest_poll"))
        self.db.commit()

        extra = ""
        if STRATEGY == "trailing" and self.watermark is not None:
            etiqueta = "mín" if self.state == ESPERANDO_COMPRA else "máx"
            extra = " | %s=%.2f" % (etiqueta, self.watermark)
        log.info("%s = %.2f | estado=%s%s", SYMBOL, price, self.state, extra)

        if self.pending_order_id is not None or self.dry_run_pending is not None:
            self.check_pending()
            return

        if STRATEGY == "trailing":
            self.decide_trailing(price)
        else:
            self.decide_fixed(price)

    # --------------------------------------------------------
    # Loop principal
    # --------------------------------------------------------
    def run(self):
        self.startup()
        log.info("Iniciando loop (Ctrl+C o botón Stop en Colab para detener).")
        while True:
            try:
                is_open, next_open = self.market_is_open()
                if not is_open:
                    log.info("Mercado cerrado (próxima apertura: %s). Durmiendo %ds.",
                             next_open, CLOSED_MARKET_SLEEP)
                    time.sleep(CLOSED_MARKET_SLEEP)
                    continue
                self.step()
                time.sleep(POLL_SECONDS)
            except KeyboardInterrupt:
                log.info("Detenido por el usuario. Estado persistido.")
                return
            except Exception:
                log.exception("Error inesperado; el bot continúa.")
                time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    setup_logging()
    if not DRY_RUN:
        log.warning("ATENCIÓN: DRY_RUN=false — se enviarán ÓRDENES REALES.")
    Bot().run()
