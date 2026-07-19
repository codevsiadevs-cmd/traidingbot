# traidingbot

Bot de trading sobre [Alpaca](https://alpaca.markets) — acciones y cripto, 2 estrategias.

## Estrategias

| `STRATEGY` | Comportamiento |
|---|---|
| `fixed` | Compra si precio ≤ `BUY_PRICE`; vende si ≥ `SELL_PRICE` |
| `trailing` | Compra al rebote desde un mínimo (`REBOUND_PCT` %); vende al caer desde un máximo (`DROP_PCT` %) |

## Setup

```bash
python -m venv .venv
# Windows:
.venv\Scripts\Activate.ps1
# Linux/Mac:
source .venv/bin/activate

pip install -r requirements.txt
cp .env.example .env
# Editar .env y pegar tus keys de Alpaca
python bot.py
```

## Seguridad

- `DRY_RUN=true` por defecto: **no envía órdenes reales**.
- Nunca commitees el archivo `.env`.
- Keys Paper empiezan con `PK`; Live con `AK`.

## Símbolos

- Acciones: `AMD`, `AAPL` (solo horario de mercado)
- Cripto: `BTC/USD` (24/7)
