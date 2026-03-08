"""
Order Flow Analysis Trading Engine - v3.4.2
+ PaperBroker + TradeLogger + BinanceMarketDataAdapter + ClawWorker

Architecture:
  BinanceMarketDataAdapter -> SignalEngine -> PaperBroker -> TradeLogger

Symbols: SOL only for first overnight run. MON disabled pending spot listing confirmation.
Runner:  ClawWorker (Railway worker process)

Environment variables:
  LOG_FILE        optional path for structured JSON log output
  BINANCE_WS_URL  override WebSocket base URL (default: wss://stream.binance.com:9443/stream?streams=)

Thread safety:
  Each SignalEngine has a dedicated threading.Lock().
  All engine mutations (add_trade, set_book_snapshot) and reads
  (tick, generate_signal, get_warmup_state) acquire this lock.

Reconnect:
  run_forever() alone is not trusted for overnight reliability.
  last_message_ts is reset on _launch() to prevent reconnect storms.
  ClawWorker supervisor monitors staleness and forces restart if no message
  arrives within STALE_CONNECTION_SEC. A per-adapter _restarting flag
  prevents overlapping restart attempts.

Logging:
  Signal logs throttled — only emitted on signal direction change.
  Warmup and tradability state transitions logged separately.
  Heartbeat includes broker summary, unrealized PnL, adapter status,
  warmup state, and last signal per symbol.
  Trade open/close and forward returns always logged.

Forward returns:
  Scheduled only on signal direction change into non-neutral state.
  Each horizon logged exactly once per signal via logged_horizons set.

MON status:
  MONUSDT perpetual futures confirmed on Binance.
  MONUSDT spot listing NOT confirmed from official Binance sources.
  MON is commented out in SYMBOL_CONFIG until spot is verified.
  To verify: GET https://api.binance.com/api/v3/exchangeInfo and check
  for MONUSDT in symbols[].symbol before uncommenting.
"""

import os
import math
import time
import json
import uuid
import threading
from dataclasses import dataclass, field
from typing import Optional, Tuple, Dict, List, Set
from collections import deque
from enum import Enum

import websocket  # pip install websocket-client


# ════════════════════════════════════════════════
#   ENVIRONMENT
# ════════════════════════════════════════════════

BINANCE_WS_BASE = os.environ.get(
    "BINANCE_WS_URL",
    "wss://stream.binance.com:9443/stream?streams="
)
STALE_CONNECTION_SEC = 30.0
HEARTBEAT_INTERVAL_SEC = 60.0


# ════════════════════════════════════════════════
#   SYMBOL CONFIG
#
#   MON commented out — spot listing unconfirmed on Binance.
#   Verify via: GET https://api.binance.com/api/v3/exchangeInfo
#   Check for MONUSDT in symbols[].symbol before enabling.
#
#   tradability/scoring set to None = use default configs.
#   Pass TradabilityConfig/ScoringConfig instances here for
#   per-symbol tuning once replay data is available.
# ════════════════════════════════════════════════

SYMBOL_CONFIG: Dict[str, Dict] = {
    "SOL": {
        "exchange_symbol": "solusdt",
        "tradability": None,
        "scoring": None,
    },
    # "MON": {
    #     "exchange_symbol": "monusdt",  # DISABLED: spot listing unconfirmed
    #     "tradability": None,
    #     "scoring": None,
    # },
}

SYMBOLS = list(SYMBOL_CONFIG.keys())
FORWARD_HORIZONS = [3.0, 10.0, 30.0, 60.0]  # seconds


# ════════════════════════════════════════════════
#   ENUMS
# ════════════════════════════════════════════════

class Signal(Enum):
    BUY = "BUY"
    SELL = "SELL"
    NEUTRAL = "NEUTRAL"


class WarmupState(Enum):
    """
    Tracks whether the engine has accumulated enough data to produce reliable signals.

    All four conditions must be true before the engine exits WARMING_UP:
      1. At least one completed delta window (window_deltas has >= 1 entry)
      2. A recent order book snapshot (within max_snapshot_age_ms)
      3. A recent trade (within max_trade_age_ms)
      4. At least one VWAP calculation (last_calculation_ts > 0)

    This is by design. Signals before warm-up are suppressed via _is_tradable_locked().
    WarmupState makes that suppression explicit and queryable by callers.

    Note: READY != TRADABLE. The engine can be READY but the market still UNTRADABLE
    (e.g. spread out of range, insufficient recent notional). These are separate states.
    """
    WARMING_UP = "WARMING_UP"
    READY = "READY"


# ════════════════════════════════════════════════
#   CORE DATA STRUCTURES
# ════════════════════════════════════════════════

@dataclass
class Trade:
    timestamp: float
    price: float
    quantity: float
    side: str

    def __post_init__(self):
        if self.price <= 0 or self.quantity <= 0:
            raise ValueError("Invalid trade")
        if self.side not in ('buy', 'sell'):
            raise ValueError("Invalid side")

    @property
    def notional(self) -> float:
        return self.price * self.quantity


@dataclass
class OrderBookSnapshot:
    timestamp: float
    bids: dict  # price -> qty
    asks: dict

    def get_mid_price(self) -> Optional[float]:
        if not self.bids or not self.asks:
            return None
        return (max(self.bids.keys()) + min(self.asks.keys())) / 2

    def get_spread(self) -> Optional[float]:
        if not self.bids or not self.asks:
            return None
        return min(self.asks.keys()) - max(self.bids.keys())

    def get_spread_bps(self) -> Optional[float]:
        mid = self.get_mid_price()
        sp = self.get_spread()
        return None if mid is None or sp is None else (sp / mid) * 10000

    def get_top_n_notional_imbalance(self, n: int = 5) -> Optional[float]:
        sb = sorted(self.bids.items(), reverse=True)[:n]
        sa = sorted(self.asks.items())[:n]
        if not sb or not sa:
            return None
        bn = sum(p * q for p, q in sb)
        an = sum(p * q for p, q in sa)
        tot = bn + an
        return None if tot == 0 else (bn - an) / tot

    def get_top_level_notional_depth(self) -> Optional[Tuple[float, float]]:
        if not self.bids or not self.asks:
            return None
        top_bid = max(self.bids.items(), key=lambda x: x[0])
        top_ask = min(self.asks.items(), key=lambda x: x[0])
        return top_bid[0] * top_bid[1], top_ask[0] * top_ask[1]

    @property
    def best_ask(self) -> Optional[float]:
        return min(self.asks.keys()) if self.asks else None

    @property
    def best_bid(self) -> Optional[float]:
        return max(self.bids.keys()) if self.bids else None


# ════════════════════════════════════════════════
#   SIGNAL ENGINE INTERNALS
# ════════════════════════════════════════════════

@dataclass
class VWAPCalculator:
    window_seconds: float = 60.0
    trades: deque = field(default_factory=deque)
    last_calculation_ts: float = 0.0
    last_vwap: Optional[float] = None

    def add_trade(self, trade: Trade):
        self.trades.append(trade)
        cutoff = trade.timestamp - self.window_seconds
        while self.trades and self.trades[0].timestamp < cutoff:
            self.trades.popleft()

    def tick(self, now_ts: float):
        if now_ts - self.last_calculation_ts >= 4.0:
            self.last_vwap = self.calculate()
            self.last_calculation_ts = now_ts

    def calculate(self) -> Optional[float]:
        if not self.trades:
            return None
        tn = sum(t.notional for t in self.trades)
        tq = sum(t.quantity for t in self.trades)
        return None if tq == 0 else tn / tq

    def get_distance_pct(self, price: float) -> float:
        v = self.last_vwap
        if v is None or v <= 0:
            return 0.0
        return (price - v) / v * 100


@dataclass
class NotionalDeltaCalculator:
    window_sec: float = 3.0
    trades: deque = field(default_factory=deque)
    window_deltas: deque = field(default_factory=lambda: deque(maxlen=4))
    last_window_start_ts: float = 0.0
    last_completed_start_ts: float = 0.0
    last_completed_end_ts: float = 0.0

    def add_trade(self, trade: Trade):
        self.trades.append(trade)

    def tick(self, now_ts: float):
        if self.last_window_start_ts == 0.0:
            self.last_window_start_ts = math.floor(now_ts / self.window_sec) * self.window_sec
            self.last_completed_start_ts = self.last_window_start_ts - self.window_sec
            self.last_completed_end_ts = self.last_window_start_ts
            return

        while now_ts >= self.last_window_start_ts + self.window_sec:
            start = self.last_window_start_ts
            end = start + self.window_sec
            delta = self._norm_delta_in_range(start, end)
            self.window_deltas.append(delta)
            self.last_completed_start_ts = start
            self.last_completed_end_ts = end
            self.last_window_start_ts = end

        cutoff = self.last_window_start_ts - self.window_sec * 3
        while self.trades and self.trades[0].timestamp < cutoff:
            self.trades.popleft()

    def _norm_delta_in_range(self, start: float, end: float) -> float:
        w = [t for t in self.trades if start <= t.timestamp < end]
        if not w:
            return 0.0
        tot = sum(t.notional for t in w)
        if tot == 0:
            return 0.0
        buy = sum(t.notional for t in w if t.side == 'buy')
        return (buy - (tot - buy)) / tot

    def get_recent_delta(self) -> float:
        return self.window_deltas[-1] if self.window_deltas else 0.0

    def get_accel(self) -> float:
        if len(self.window_deltas) < 2:
            return 0.0
        return self.window_deltas[-1] - self.window_deltas[-2]

    def get_recent_window_trade_count(self) -> int:
        s, e = self.last_completed_start_ts, self.last_completed_end_ts
        if e <= s:
            return 0
        return len([t for t in self.trades if s <= t.timestamp < e])

    def get_recent_window_notional(self) -> float:
        s, e = self.last_completed_start_ts, self.last_completed_end_ts
        if e <= s:
            return 0.0
        return sum(t.notional for t in self.trades if s <= t.timestamp < e)

    @property
    def has_completed_window(self) -> bool:
        return len(self.window_deltas) > 0


# ════════════════════════════════════════════════
#   CONFIG
# ════════════════════════════════════════════════

@dataclass
class TradabilityConfig:
    """
    Gates for whether market conditions are suitable for signal generation.
    Tune based on symbol liquidity profile — independent of signal performance.

    Note on timestamps: trade events use exchange time (data["T"]),
    order book snapshots use local time.time() since @depth20 payloads
    carry no event timestamp. Staleness checks reflect this mixed sourcing.
    """
    min_spread_bps: float = 0.8
    max_spread_bps: float = 35.0
    min_trades_in_window: int = 3
    min_notional_in_window: float = 150.0
    min_top_level_notional: float = 500.0
    max_snapshot_age_ms: float = 600.0
    max_trade_age_ms: float = 1500.0


@dataclass
class ScoringConfig:
    """
    Parameters controlling signal scoring and viability.
    Primary targets for per-symbol tuning once replay data is available.

    WARNING: dominance_ratio and signal_threshold are priors, not fitted values.
    Both are prime candidates for per-symbol tuning after replay analysis.
    """
    min_abs_imbalance: float = 0.085
    min_evidence_total: float = 0.75
    viability_threshold: float = 0.32
    dominance_ratio: float = 1.42
    signal_threshold: float = 0.68


# ════════════════════════════════════════════════
#   SIGNAL OUTPUT
# ════════════════════════════════════════════════

@dataclass
class SignalOutput:
    signal: Signal
    score: float
    reasoning: str
    book_imbalance_notional: Optional[float]
    vwap_dist_pct: float
    delta_recent: float
    delta_accel: float
    spread_bps: Optional[float]
    tradable: bool
    rejection_reason: Optional[str] = None


# ════════════════════════════════════════════════
#   SIGNAL ENGINE  (thread-safe via per-instance lock)
# ════════════════════════════════════════════════

class SignalEngine:
    def __init__(
        self,
        tradability_config: Optional[TradabilityConfig] = None,
        scoring_config: Optional[ScoringConfig] = None,
    ):
        self.tradability = tradability_config or TradabilityConfig()
        self.scoring = scoring_config or ScoringConfig()
        self.delta_calc = NotionalDeltaCalculator()
        self.vwap_calc = VWAPCalculator()
        self.last_book: Optional[OrderBookSnapshot] = None
        self.last_trade_ts: Optional[float] = None
        self._lock = threading.Lock()

    def add_trade(self, trade: Trade):
        with self._lock:
            self.delta_calc.add_trade(trade)
            self.vwap_calc.add_trade(trade)
            self.last_trade_ts = trade.timestamp

    def set_book_snapshot(self, snapshot: OrderBookSnapshot):
        with self._lock:
            self.last_book = snapshot

    def tick(self, now_ts: float):
        with self._lock:
            self.delta_calc.tick(now_ts)
            self.vwap_calc.tick(now_ts)

    def get_warmup_state(self, now_ts: float) -> WarmupState:
        with self._lock:
            return self._warmup_state_locked(now_ts)

    def is_ready(self, now_ts: float) -> bool:
        return self.get_warmup_state(now_ts) == WarmupState.READY

    def generate_signal(self, now_ts: float) -> SignalOutput:
        with self._lock:
            return self._generate_locked(now_ts)

    def snapshot_book(self) -> Optional[OrderBookSnapshot]:
        """
        Thread-safe retrieval of current snapshot reference.
        OrderBookSnapshot is replaced atomically as a whole object,
        so returning the reference under lock is safe for read-only use.
        """
        with self._lock:
            return self.last_book

    def _warmup_state_locked(self, now_ts: float) -> WarmupState:
        cfg = self.tradability
        if not self.delta_calc.has_completed_window:
            return WarmupState.WARMING_UP
        if self.vwap_calc.last_calculation_ts == 0.0:
            return WarmupState.WARMING_UP
        if self.last_book is None:
            return WarmupState.WARMING_UP
        if (now_ts - self.last_book.timestamp) * 1000 > cfg.max_snapshot_age_ms:
            return WarmupState.WARMING_UP
        if self.last_trade_ts is None:
            return WarmupState.WARMING_UP
        if (now_ts - self.last_trade_ts) * 1000 > cfg.max_trade_age_ms:
            return WarmupState.WARMING_UP
        return WarmupState.READY

    def _is_tradable_locked(self, now_ts: float) -> Tuple[bool, Optional[str]]:
        cfg = self.tradability
        if self.last_book is None:
            return False, "no_book_snapshot"
        if (now_ts - self.last_book.timestamp) * 1000 > cfg.max_snapshot_age_ms:
            return False, "book_stale"
        if self.last_trade_ts is None:
            return False, "no_trades_seen"
        if (now_ts - self.last_trade_ts) * 1000 > cfg.max_trade_age_ms:
            return False, "trades_stale"
        sp = self.last_book.get_spread_bps()
        if sp is None:
            return False, "spread_cannot_compute"
        if sp < cfg.min_spread_bps or sp > cfg.max_spread_bps:
            return False, f"spread_out_of_range_{sp:.1f}bps"
        if self.delta_calc.get_recent_window_trade_count() < cfg.min_trades_in_window:
            return False, "insufficient_recent_trades"
        if self.delta_calc.get_recent_window_notional() < cfg.min_notional_in_window:
            return False, "insufficient_recent_notional"
        depth_n = self.last_book.get_top_level_notional_depth()
        if depth_n is None or min(depth_n) < cfg.min_top_level_notional:
            return False, "top_level_too_thin"
        return True, None

    def _generate_locked(self, now_ts: float) -> SignalOutput:
        tradable, reason = self._is_tradable_locked(now_ts)
        if not tradable:
            return SignalOutput(Signal.NEUTRAL, 0.0, f"untradable: {reason}",
                                None, 0.0, 0.0, 0.0, None, False, reason)

        scfg = self.scoring
        imb = self.last_book.get_top_n_notional_imbalance(n=5) or 0.0
        mid = self.last_book.get_mid_price() or 0.0
        vwap_d = self.vwap_calc.get_distance_pct(mid)
        d_rec = self.delta_calc.get_recent_delta()
        accel = self.delta_calc.get_accel()
        sp_bps = self.last_book.get_spread_bps() or 0.0

        buy_v, sell_v = [], []
        if abs(imb) >= scfg.min_abs_imbalance:
            s = abs(imb) * 6.5
            (buy_v if imb > 0 else sell_v).append(s)
        if abs(d_rec) > 0.065:
            s = abs(d_rec) * 5.5
            (buy_v if d_rec > 0 else sell_v).append(s)
        if abs(accel) > 0.035:
            s = abs(accel) * 4.8
            (buy_v if accel > 0 else sell_v).append(s)

        bt = sum(buy_v)
        st = sum(sell_v)
        tev = bt + st

        vwap_fresh = (now_ts - self.vwap_calc.last_calculation_ts) < 12

        if tev < scfg.min_evidence_total or not vwap_fresh:
            raw_dir = Signal.NEUTRAL
            raw_strength = 0.0
        else:
            dom = max(bt, st) / tev if tev > 0 else 0
            mag = 1 / (1 + math.exp(-(tev - 1.2) / 0.9))
            raw_strength = dom * mag
            if bt > st * scfg.dominance_ratio:
                raw_dir = Signal.BUY
            elif st > bt * scfg.dominance_ratio:
                raw_dir = Signal.SELL
            else:
                raw_dir = Signal.NEUTRAL
                raw_strength = 0.0

        final_score = raw_strength

        if vwap_d > 1.0:
            final_score *= 0.70 if raw_dir == Signal.BUY else 1.13
        elif vwap_d > 0.4:
            final_score *= 0.84 if raw_dir == Signal.BUY else 1.09
        elif vwap_d < -1.0:
            final_score *= 0.70 if raw_dir == Signal.SELL else 1.13
        elif vwap_d < -0.4:
            final_score *= 0.84 if raw_dir == Signal.SELL else 1.09
        elif abs(vwap_d) < 0.35:
            final_score *= 1.05

        # Vol proxy: log-return stddev over completed window when sufficient trades exist.
        # TEMPORARY HEURISTIC: fallback blends vwap_d and accel as vol surrogates.
        # Replace with proper rolling realized-vol estimate once replay data is available.
        recent_prices = [
            t.price for t in self.delta_calc.trades
            if self.delta_calc.last_completed_start_ts <= t.timestamp
            < self.delta_calc.last_completed_end_ts
        ]
        if len(recent_prices) >= 3:
            rets = [math.log(recent_prices[i + 1] / recent_prices[i])
                    for i in range(len(recent_prices) - 1)]
            vol_proxy = math.sqrt(sum(r * r for r in rets) / max(1, len(rets))) * 10000
        else:
            vol_proxy = abs(vwap_d) * 2 + abs(accel) * 4

        cost_proxy = sp_bps / 10000 * 12
        expected_edge = final_score * (1 + vol_proxy / 50) - cost_proxy

        if expected_edge < scfg.viability_threshold:
            final_score *= 0.60
            viability = "edge_below_threshold"
        else:
            viability = "edge_ok"

        final_score = max(0.0, min(1.0, final_score))
        sig = raw_dir if final_score >= scfg.signal_threshold else Signal.NEUTRAL

        parts = [
            f"imb_n:{imb:.3f}",
            f"vwap_d:{vwap_d:.2f}%",
            f"delta:{d_rec:.3f}",
            f"accel:{accel:.3f}",
            f"raw:{raw_dir.value}({raw_strength:.2f})",
            f"score:{sig.value}({final_score:.2f})",
            viability,
            "vwap_fresh" if vwap_fresh else "vwap_stale"
        ]

        return SignalOutput(
            signal=sig,
            score=final_score,
            reasoning=" | ".join(parts),
            book_imbalance_notional=imb,
            vwap_dist_pct=vwap_d,
            delta_recent=d_rec,
            delta_accel=accel,
            spread_bps=sp_bps,
            tradable=True
        )


# ════════════════════════════════════════════════
#   PAPER BROKER
# ════════════════════════════════════════════════

@dataclass
class PaperPosition:
    symbol: str
    side: str
    entry_price: float
    quantity: float
    entry_ts: float
    entry_fee: float
    trade_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    # MFE/MAE stored as raw price difference from entry.
    # pct = value / entry_price    dollar = value * quantity
    max_favorable_excursion: float = 0.0
    max_adverse_excursion: float = 0.0

    def unrealized_pnl(self, current_price: float) -> float:
        return (current_price - self.entry_price) * self.quantity

    def update_excursions(self, current_price: float):
        move = current_price - self.entry_price
        self.max_favorable_excursion = max(self.max_favorable_excursion, move)
        self.max_adverse_excursion = min(self.max_adverse_excursion, move)


@dataclass
class PaperBrokerConfig:
    initial_cash: float = 10_000.0
    fee_rate: float = 0.0006
    slippage_bps: float = 1.0
    max_position_usd: float = 500.0
    cooldown_sec: float = 10.0
    max_hold_sec: float = 60.0
    stop_loss_pct: float = 0.004
    take_profit_pct: float = 0.008
    long_only: bool = True


class PaperBroker:
    """
    Long-only paper broker.
    SELL signals are logged and tracked for forward returns but do not open positions.
    Shorts will be added after long-only baseline results are reviewed.
    """

    def __init__(self, config: Optional[PaperBrokerConfig] = None):
        self.config = config or PaperBrokerConfig()
        self.cash = self.config.initial_cash
        self.positions: Dict[str, PaperPosition] = {}
        self.realized_pnl: float = 0.0
        self.peak_equity: float = self.config.initial_cash
        self.max_drawdown: float = 0.0
        self.last_entry_ts: Dict[str, float] = {}
        self.closed_trades: List[dict] = []

    def handle(self, symbol: str, signal: SignalOutput, book: OrderBookSnapshot,
               now_ts: float, logger: "TradeLogger"):
        position = self.positions.get(symbol)
        mid = book.get_mid_price()
        if position and mid:
            position.update_excursions(mid)
        if position:
            exit_reason = self._check_exit(position, book, now_ts)
            if exit_reason:
                self._close_position(symbol, book, now_ts, exit_reason, logger)
                return
        if signal.signal == Signal.BUY and not position:
            cfg = self.config
            cooldown_ok = (now_ts - self.last_entry_ts.get(symbol, 0)) >= cfg.cooldown_sec
            if cooldown_ok and signal.tradable:
                self._open_long(symbol, book, now_ts, signal, logger)

    def _check_exit(self, position: PaperPosition, book: OrderBookSnapshot,
                    now_ts: float) -> Optional[str]:
        cfg = self.config
        bid = book.best_bid
        if bid is None:
            return None
        move_pct = (bid - position.entry_price) / position.entry_price
        if move_pct <= -cfg.stop_loss_pct:
            return "stop_loss"
        if move_pct >= cfg.take_profit_pct:
            return "take_profit"
        if (now_ts - position.entry_ts) >= cfg.max_hold_sec:
            return "time_exit"
        return None

    def _open_long(self, symbol: str, book: OrderBookSnapshot, now_ts: float,
                   signal: SignalOutput, logger: "TradeLogger"):
        cfg = self.config
        ask = book.best_ask
        if ask is None:
            return
        slippage = ask * (cfg.slippage_bps / 10000)
        fill_price = ask + slippage
        quantity = min(cfg.max_position_usd, self.cash) / fill_price
        cost = fill_price * quantity
        fee = cost * cfg.fee_rate
        if cost + fee > self.cash:
            return
        self.cash -= (cost + fee)
        position = PaperPosition(
            symbol=symbol, side='long', entry_price=fill_price,
            quantity=quantity, entry_ts=now_ts, entry_fee=fee,
        )
        self.positions[symbol] = position
        self.last_entry_ts[symbol] = now_ts
        logger.log_trade_open(symbol, position, signal)

    def _close_position(self, symbol: str, book: OrderBookSnapshot, now_ts: float,
                         reason: str, logger: "TradeLogger"):
        cfg = self.config
        position = self.positions.pop(symbol)
        bid = book.best_bid
        if bid is None:
            bid = position.entry_price
        slippage = bid * (cfg.slippage_bps / 10000)
        fill_price = bid - slippage
        proceeds = fill_price * position.quantity
        exit_fee = proceeds * cfg.fee_rate
        net_proceeds = proceeds - exit_fee
        cost_basis = position.entry_price * position.quantity
        pnl = net_proceeds - cost_basis
        total_fees = position.entry_fee + exit_fee
        self.cash += net_proceeds
        self.realized_pnl += pnl
        # Update drawdown tracking
        equity = self._total_equity({})
        if equity > self.peak_equity:
            self.peak_equity = equity
        drawdown = (self.peak_equity - equity) / self.peak_equity if self.peak_equity > 0 else 0.0
        if drawdown > self.max_drawdown:
            self.max_drawdown = drawdown
        trade_record = {
            "trade_id": position.trade_id,
            "symbol": symbol,
            "side": position.side,
            "entry_ts": position.entry_ts,
            "exit_ts": now_ts,
            "hold_sec": round(now_ts - position.entry_ts, 2),
            "entry_price": position.entry_price,
            "exit_price": fill_price,
            "quantity": position.quantity,
            "entry_fee": round(position.entry_fee, 6),
            "exit_fee": round(exit_fee, 6),
            "total_fees": round(total_fees, 6),
            "pnl": round(pnl, 6),
            "exit_reason": reason,
            "mfe_price": round(position.max_favorable_excursion, 6),
            "mae_price": round(position.max_adverse_excursion, 6),
            "mfe_pct": round(position.max_favorable_excursion / position.entry_price * 100, 4),
            "mae_pct": round(position.max_adverse_excursion / position.entry_price * 100, 4),
        }
        self.closed_trades.append(trade_record)
        logger.log_trade_close(trade_record)

    def _total_equity(self, mid_prices: Dict[str, float]) -> float:
        """Cash plus unrealized PnL across all open positions."""
        unrealized = sum(
            pos.unrealized_pnl(mid_prices[sym])
            for sym, pos in self.positions.items()
            if sym in mid_prices
        )
        return self.cash + unrealized

    def summary(self, mid_prices: Optional[Dict[str, float]] = None) -> dict:
        mid_prices = mid_prices or {}
        trades = self.closed_trades
        wins = [t for t in trades if t["pnl"] > 0]
        losses = [t for t in trades if t["pnl"] <= 0]
        unrealized = sum(
            pos.unrealized_pnl(mid_prices[sym])
            for sym, pos in self.positions.items()
            if sym in mid_prices
        )
        total_equity = self.cash + unrealized
        # Update peak/drawdown on summary call too
        if total_equity > self.peak_equity:
            self.peak_equity = total_equity
        drawdown = (self.peak_equity - total_equity) / self.peak_equity if self.peak_equity > 0 else 0.0
        if drawdown > self.max_drawdown:
            self.max_drawdown = drawdown
        by_symbol: Dict[str, float] = {}
        for t in trades:
            by_symbol[t["symbol"]] = round(by_symbol.get(t["symbol"], 0.0) + t["pnl"], 6)
        return {
            "cash": round(self.cash, 4),
            "unrealized_pnl": round(unrealized, 4),
            "total_equity": round(total_equity, 4),
            "realized_pnl": round(self.realized_pnl, 4),
            "total_trades": len(trades),
            "win_rate": round(len(wins) / len(trades), 4) if trades else 0.0,
            "avg_win": round(sum(t["pnl"] for t in wins) / len(wins), 6) if wins else 0.0,
            "avg_loss": round(sum(t["pnl"] for t in losses) / len(losses), 6) if losses else 0.0,
            "avg_pnl": round(sum(t["pnl"] for t in trades) / len(trades), 6) if trades else 0.0,
            "total_fees": round(sum(t["total_fees"] for t in trades), 6),
            "max_drawdown_pct": round(self.max_drawdown * 100, 4),
            "pnl_by_symbol": by_symbol,
            "open_positions": list(self.positions.keys()),
        }


# ════════════════════════════════════════════════
#   TRADE LOGGER
# ════════════════════════════════════════════════

class TradeLogger:
    def __init__(self, log_file: Optional[str] = None, engine_version: str = "v3.4.2"):
        self.engine_version = engine_version
        self._fh = open(log_file, "a") if log_file else None

    def _emit(self, record: dict):
        record["engine"] = self.engine_version
        line = json.dumps(record)
        print(line)
        if self._fh:
            self._fh.write(line + "\n")
            self._fh.flush()

    def log_signal(self, symbol: str, signal: SignalOutput, warmup_state: WarmupState, now_ts: float):
        self._emit({
            "event": "signal",
            "ts": now_ts,
            "symbol": symbol,
            "signal": signal.signal.value,
            "score": round(signal.score, 4),
            "tradable": signal.tradable,
            "rejection_reason": signal.rejection_reason,
            "spread_bps": round(signal.spread_bps, 2) if signal.spread_bps is not None else None,
            "imbalance": round(signal.book_imbalance_notional, 4) if signal.book_imbalance_notional is not None else None,
            "delta": round(signal.delta_recent, 4),
            "accel": round(signal.delta_accel, 4),
            "vwap_dist_pct": round(signal.vwap_dist_pct, 4),
            "warmup": warmup_state.value,
            "reasoning": signal.reasoning,
        })

    def log_state_transition(self, symbol: str, event: str, detail: str, now_ts: float):
        self._emit({"event": event, "symbol": symbol, "detail": detail, "ts": now_ts})

    def log_trade_open(self, symbol: str, position: PaperPosition, signal: SignalOutput):
        self._emit({
            "event": "trade_open",
            "trade_id": position.trade_id,
            "symbol": symbol,
            "side": position.side,
            "entry_ts": position.entry_ts,
            "entry_price": round(position.entry_price, 6),
            "quantity": round(position.quantity, 6),
            "entry_fee": round(position.entry_fee, 6),
            "signal_score": round(signal.score, 4),
            "signal_reasoning": signal.reasoning,
        })

    def log_trade_close(self, record: dict):
        self._emit({"event": "trade_close", **record})

    def log_forward_return(self, symbol: str, signal_ts: float, signal: Signal,
                           horizon_sec: float, entry_price: float, forward_price: float):
        ret_pct = (forward_price - entry_price) / entry_price * 100 if entry_price else 0.0
        self._emit({
            "event": "forward_return",
            "symbol": symbol,
            "signal_ts": signal_ts,
            "signal": signal.value,
            "horizon_sec": horizon_sec,
            "entry_price": entry_price,
            "forward_price": forward_price,
            "ret_pct": round(ret_pct, 6),
        })

    def log_heartbeat(self, now_ts: float, summary: dict, adapter_status: Optional[dict] = None,
                      warmup_snapshot: Optional[dict] = None, signal_snapshot: Optional[dict] = None):
        record = {"event": "heartbeat", "ts": now_ts, **summary}
        if adapter_status:
            record["adapters"] = adapter_status
        if warmup_snapshot:
            record["warmup"] = warmup_snapshot
        if signal_snapshot:
            record["last_signal"] = signal_snapshot
        self._emit(record)

    def log_connection(self, symbol: str, event: str, detail: str = ""):
        self._emit({"event": f"ws_{event}", "symbol": symbol, "detail": detail, "ts": time.time()})

    def close(self):
        if self._fh:
            self._fh.close()


# ════════════════════════════════════════════════
#   BINANCE MARKET DATA ADAPTER
# ════════════════════════════════════════════════

class BinanceMarketDataAdapter:
    """
    Live adapter using Binance combined WebSocket streams.

    Streams per symbol:
      <symbol>@trade          individual trade events
      <symbol>@depth20@100ms  top-20 order book, 100ms cadence

    Reconnect storm protection:
      last_message_ts is reset to time.time() on every _launch() call.
      This prevents the supervisor from immediately seeing the fresh
      connection as stale before the first message arrives.

    Restart guard:
      _restarting flag (protected by _restart_lock) prevents overlapping
      restart attempts if the supervisor fires multiple times in a row.
    """

    def __init__(self, symbol: str, exchange_symbol: str, engine: SignalEngine, logger: TradeLogger):
        self.symbol = symbol
        self.exchange_symbol = exchange_symbol.lower()
        self.engine = engine
        self.logger = logger
        self._ws: Optional[websocket.WebSocketApp] = None
        self._thread: Optional[threading.Thread] = None
        self._connected = False
        self._restarting = False
        self._restart_lock = threading.Lock()
        self.last_message_ts: float = 0.0

    def start(self):
        try:
            self._launch()
        except Exception as e:
            self.logger.log_connection(self.symbol, "start_error", str(e))

    def stop(self):
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass

    def restart(self):
        with self._restart_lock:
            if self._restarting:
                return
            self._restarting = True
        try:
            self.logger.log_connection(self.symbol, "restart", "supervisor triggered stale reconnect")
            self.stop()
            time.sleep(2)
            self._launch()
        except Exception as e:
            self.logger.log_connection(self.symbol, "restart_error", str(e))
        finally:
            with self._restart_lock:
                self._restarting = False

    def is_stale(self, now_ts: float) -> bool:
        if self.last_message_ts == 0.0:
            return False
        return (now_ts - self.last_message_ts) > STALE_CONNECTION_SEC

    def _launch(self):
        # Reset last_message_ts BEFORE starting the thread to prevent
        # the supervisor from seeing the new connection as immediately stale.
        self.last_message_ts = time.time()
        streams = f"{self.exchange_symbol}@trade/{self.exchange_symbol}@depth20@100ms"
        url = BINANCE_WS_BASE + streams
        self._ws = websocket.WebSocketApp(
            url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        self._thread = threading.Thread(target=self._ws.run_forever, daemon=True)
        self._thread.start()
        self.logger.log_connection(self.symbol, "launch", url)

    def _on_open(self, ws):
        self._connected = True
        self.last_message_ts = time.time()
        self.logger.log_connection(self.symbol, "open")

    def _on_close(self, ws, code, msg):
        self._connected = False
        self.logger.log_connection(self.symbol, "close", f"{code} {msg}")

    def _on_error(self, ws, error):
        self.logger.log_connection(self.symbol, "error", str(error))

    def _on_message(self, ws, raw: str):
        self.last_message_ts = time.time()
        try:
            msg = json.loads(raw)
            stream = msg.get("stream", "")
            data = msg.get("data", {})
            if "@trade" in stream:
                self._handle_trade(data)
            elif "@depth" in stream:
                self._handle_depth(data)
        except Exception as e:
            self.logger.log_connection(self.symbol, "parse_error", str(e))

    def _handle_trade(self, data: dict):
        try:
            ts = data["T"] / 1000.0
            price = float(data["p"])
            quantity = float(data["q"])
            side = "sell" if data["m"] else "buy"
            self.engine.add_trade(Trade(timestamp=ts, price=price, quantity=quantity, side=side))
        except Exception as e:
            self.logger.log_connection(self.symbol, "trade_parse_error", str(e))

    def _handle_depth(self, data: dict):
        try:
            ts = time.time()  # @depth20 carries no event timestamp
            bids = {float(p): float(q) for p, q in data.get("bids", []) if float(q) > 0}
            asks = {float(p): float(q) for p, q in data.get("asks", []) if float(q) > 0}
            self.engine.set_book_snapshot(OrderBookSnapshot(timestamp=ts, bids=bids, asks=asks))
        except Exception as e:
            self.logger.log_connection(self.symbol, "depth_parse_error", str(e))


# ════════════════════════════════════════════════
#   CLAW WORKER
# ════════════════════════════════════════════════

class ClawWorker:
    """
    Main worker loop for Railway deployment.

    - One SignalEngine + BinanceMarketDataAdapter per symbol
    - Shared PaperBroker (long-only — SELL signals logged but not traded)
    - Signal logs throttled: only on direction change
    - Warmup and tradability state transitions logged separately
    - Forward returns scheduled only on signal direction change into non-neutral
    - Per-horizon deduplication on forward return logging
    - Heartbeat includes broker summary with unrealized PnL, adapter status,
      warmup snapshot, and last signal per symbol
    - Supervisor checks for stale connections every tick
    - Per-symbol try/except — one bad tick cannot kill the session
    - _running flag for graceful shutdown
    - Graceful finally block closes WebSockets and logger
    """

    def __init__(self, log_file: Optional[str] = None):
        self.logger = TradeLogger(log_file=log_file)
        self.broker = PaperBroker()
        self.engines: Dict[str, SignalEngine] = {
            s: SignalEngine(
                tradability_config=SYMBOL_CONFIG[s].get("tradability"),
                scoring_config=SYMBOL_CONFIG[s].get("scoring"),
            )
            for s in SYMBOLS
        }
        self.adapters: Dict[str, BinanceMarketDataAdapter] = {
            s: BinanceMarketDataAdapter(
                s, SYMBOL_CONFIG[s]["exchange_symbol"], self.engines[s], self.logger,
            )
            for s in SYMBOLS
        }
        self._last_signal: Dict[str, Signal] = {s: Signal.NEUTRAL for s in SYMBOLS}
        self._last_warmup: Dict[str, Optional[WarmupState]] = {s: None for s in SYMBOLS}
        self._last_tradable: Dict[str, Optional[bool]] = {s: None for s in SYMBOLS}
        self._last_heartbeat_ts: float = 0.0
        self._running = False
        self._pending_forward: Dict[str, List[Tuple[float, Signal, float, Set[float]]]] = {
            s: [] for s in SYMBOLS
        }

    def run(self, tick_interval_sec: float = 0.5):
        cfg = self.broker.config
        print(json.dumps({
            "event": "startup",
            "engine": "v3.4.2",
            "symbols": SYMBOLS,
            "mode": "long_only_paper",
            "ws_base": BINANCE_WS_BASE,
            "broker_config": {
                "initial_cash": cfg.initial_cash,
                "fee_rate": cfg.fee_rate,
                "slippage_bps": cfg.slippage_bps,
                "stop_loss_pct": cfg.stop_loss_pct,
                "take_profit_pct": cfg.take_profit_pct,
                "max_hold_sec": cfg.max_hold_sec,
                "max_position_usd": cfg.max_position_usd,
                "cooldown_sec": cfg.cooldown_sec,
            },
            "note": "SELL signals logged and tracked for forward returns but do not open positions",
            "mon_status": "disabled — MONUSDT spot listing unconfirmed on Binance",
        }))

        for adapter in self.adapters.values():
            adapter.start()

        self._running = True
        try:
            while self._running:
                now_ts = time.time()
                self._supervise_connections(now_ts)
                for symbol in SYMBOLS:
                    try:
                        self._tick_symbol(symbol, now_ts)
                    except Exception as e:
                        self.logger.log_connection(symbol, "worker_error", str(e))
                if now_ts - self._last_heartbeat_ts >= HEARTBEAT_INTERVAL_SEC:
                    self._emit_heartbeat(now_ts)
                    self._last_heartbeat_ts = now_ts
                time.sleep(tick_interval_sec)
        except KeyboardInterrupt:
            print("[ClawWorker] KeyboardInterrupt received — shutting down.")
        finally:
            self._running = False
            for adapter in self.adapters.values():
                adapter.stop()
            self._emit_heartbeat(time.time())
            self.logger.close()
            print("[ClawWorker] Shut down cleanly.")

    def stop(self):
        self._running = False

    def _supervise_connections(self, now_ts: float):
        for symbol, adapter in self.adapters.items():
            if adapter.is_stale(now_ts):
                adapter.restart()

    def _emit_heartbeat(self, now_ts: float):
        mid_prices = {}
        for symbol in SYMBOLS:
            book = self.engines[symbol].snapshot_book()
            if book:
                mid = book.get_mid_price()
                if mid:
                    mid_prices[symbol] = mid

        adapter_status = {
            s: {
                "connected": self.adapters[s]._connected,
                "msg_age_sec": round(now_ts - self.adapters[s].last_message_ts, 1)
                    if self.adapters[s].last_message_ts > 0 else None,
            }
            for s in SYMBOLS
        }
        warmup_snapshot = {
            s: self._last_warmup[s].value if self._last_warmup[s] else None
            for s in SYMBOLS
        }
        signal_snapshot = {s: self._last_signal[s].value for s in SYMBOLS}

        self.logger.log_heartbeat(
            now_ts,
            self.broker.summary(mid_prices),
            adapter_status=adapter_status,
            warmup_snapshot=warmup_snapshot,
            signal_snapshot=signal_snapshot,
        )

    def _tick_symbol(self, symbol: str, now_ts: float):
        engine = self.engines[symbol]
        engine.tick(now_ts)

        warmup = engine.get_warmup_state(now_ts)
        signal = engine.generate_signal(now_ts)

        # Log warmup state transitions
        if warmup != self._last_warmup[symbol]:
            self.logger.log_state_transition(
                symbol, "warmup_transition",
                f"{self._last_warmup[symbol]} -> {warmup.value}", now_ts
            )
            self._last_warmup[symbol] = warmup

        # Log tradability transitions
        if signal.tradable != self._last_tradable[symbol]:
            detail = f"tradable={signal.tradable}"
            if signal.rejection_reason:
                detail += f" reason={signal.rejection_reason}"
            self.logger.log_state_transition(symbol, "tradability_transition", detail, now_ts)
            self._last_tradable[symbol] = signal.tradable

        prev_signal = self._last_signal[symbol]
        book = engine.snapshot_book()

        # On signal direction change: log and schedule forward returns
        if signal.signal != prev_signal:
            self.logger.log_signal(symbol, signal, warmup, now_ts)
            if warmup == WarmupState.READY and book is not None:
                mid = book.get_mid_price()
                if signal.signal != Signal.NEUTRAL and mid:
                    self._pending_forward[symbol].append(
                        (now_ts, signal.signal, mid, set())
                    )
            self._last_signal[symbol] = signal.signal

        # Broker acts on current signal every tick regardless of direction change
        if warmup == WarmupState.READY and book is not None:
            self.broker.handle(symbol, signal, book, now_ts, self.logger)

        self._resolve_forward_returns(symbol, engine, now_ts)

    def _resolve_forward_returns(self, symbol: str, engine: SignalEngine, now_ts: float):
        book = engine.snapshot_book()
        if book is None:
            return
        forward_price = book.get_mid_price()
        if forward_price is None:
            return

        max_horizon = max(FORWARD_HORIZONS)
        still_pending = []

        for (signal_ts, sig, entry_price, logged) in self._pending_forward[symbol]:
            elapsed = now_ts - signal_ts
            for horizon in FORWARD_HORIZONS:
                if horizon not in logged and abs(elapsed - horizon) < 0.6:
                    self.logger.log_forward_return(
                        symbol, signal_ts, sig, horizon, entry_price, forward_price
                    )
                    logged.add(horizon)
            if elapsed < max_horizon + 1.0:
                still_pending.append((signal_ts, sig, entry_price, logged))

        self._pending_forward[symbol] = still_pending


# ════════════════════════════════════════════════
#   ENTRY POINT
# ════════════════════════════════════════════════

if __name__ == "__main__":
    log_file = os.environ.get("LOG_FILE", None)
    worker = ClawWorker(log_file=log_file)
    worker.run()
