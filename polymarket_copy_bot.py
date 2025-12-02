#!/usr/bin/env python3
"""
Polymarket Copy Trading Bot
===========================

⚠️  FINANCIAL RISK WARNING ⚠️
This script carries significant financial risk. Only run this bot with funds you can
afford to lose completely. Automated trading can result in rapid losses due to:
- Market volatility, API errors or delays, bugs in the code, network issues, liquidity problems

NEVER share your private key with anyone. Keep it secure at all times.

Target Wallet: 0xeffcc79a8572940cee2238b44eac89f2c48fda88

REQUIREMENTS
============
Python 3.10+

Dependencies (requirements.txt):
    requests>=2.31.0
    py-clob-client>=0.1.0
    python-dotenv>=1.0.0

INSTALLATION
============
1. Install dependencies:
   pip install -r requirements.txt

2. Create .env file with your configuration:
   POLYMARKET_PRIVATE_KEY=your_private_key_without_0x_prefix
   MAX_TRADE_USDC=100.0
   RISK_MULTIPLIER=1.0
   POLL_INTERVAL_SECONDS=2.0
   DRY_RUN=false
   LOG_LEVEL=INFO

3. Run the bot:
   python polymarket_copy_bot.py

USAGE
=====
- Start in DRY_RUN=true mode first to test without placing real orders
- Monitor the logs closely when running live
- Press Ctrl+C to stop gracefully

API DOCUMENTATION
=================
- Polymarket Data API: https://docs.polymarket.com/developers/misc-endpoints/data-api-activity
- Polymarket CLOB API: https://docs.polymarket.com/developers/CLOB/orders/create-order
- py-clob-client: https://github.com/Polymarket/py-clob-client
"""

import os
import sys
import time
import json
import logging
import threading
from dataclasses import dataclass
from typing import Optional, Dict, List
from datetime import datetime, timedelta
from pathlib import Path

# Standard library + requests for HTTP
import requests
from dotenv import load_dotenv

# Polymarket CLOB client for order placement
try:
    from py_clob_client.client import ClobClient  # type: ignore
    from py_clob_client.clob_types import OrderArgs  # type: ignore
    from py_clob_client.order_builder.constants import BUY, SELL  # type: ignore
except ImportError:
    print("ERROR: py-clob-client not installed. Run: pip install py-clob-client")
    sys.exit(1)

try:
    # Fix timezone issue before importing Application
    # Patch tzlocal.get_localzone to return pytz timezone
    import pytz
    import tzlocal
    
    # Store original function
    original_get_localzone = tzlocal.get_localzone
    
    # Create patched version that returns pytz UTC
    def patched_get_localzone():
        return pytz.UTC
    
    # Replace the function
    tzlocal.get_localzone = patched_get_localzone
    
    from telegram import Update
    from telegram.ext import Application, CommandHandler, ContextTypes
    from telegram.ext import JobQueue
except ImportError:
    print("ERROR: python-telegram-bot not installed. Run: pip install python-telegram-bot")
    sys.exit(1)

# Load environment variables
load_dotenv()

# ============================================================================
# CONFIGURATION
# ============================================================================

TARGET_WALLET = "0xeffcc79a8572940cee2238b44eac89f2c48fda88"
DATA_API_BASE_URL = "https://data-api.polymarket.com"
CLOB_API_BASE_URL = "https://clob.polymarket.com"

# Read from environment variables with defaults
POLYMARKET_PRIVATE_KEY = os.getenv("POLYMARKET_PRIVATE_KEY", "")
MAX_TRADE_USDC = float(os.getenv("MAX_TRADE_USDC", "100.0"))
RISK_MULTIPLIER = float(os.getenv("RISK_MULTIPLIER", "1.0"))
POLL_INTERVAL_SECONDS = float(os.getenv("POLL_INTERVAL_SECONDS", "2.0"))
DRY_RUN = os.getenv("DRY_RUN", "false").lower() in ("true", "1", "yes")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8236934064:AAEcPDDIilSw9W4-TzO26Abefe27dDRxDpQ")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")  # Optional: for backward compatibility

# State file for persistence
STATE_FILE = Path("bot_state.json")

# Retry configuration
MAX_RETRIES = 3
INITIAL_RETRY_DELAY = 1.0
MAX_RETRY_DELAY = 60.0

# ============================================================================
# LOGGING SETUP
# ============================================================================

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('polymarket_bot.log', mode='a')
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# DATA MODELS
# ============================================================================

@dataclass
class Trade:
    """
    Represents a trade from Polymarket.

    Fields map to Polymarket Data API /activity endpoint response.
    """
    transaction_hash: str
    timestamp: int
    market_id: str           # conditionId or market identifier
    token_id: str            # asset/outcome token ID
    outcome: str             # Human-readable outcome (e.g., "Up" or "Down")
    side: str                # "BUY" or "SELL"
    size: float              # Number of shares
    price: float             # Price per share (0-1 range typically)
    proxy_wallet: str
    title: str = ""          # Market title (e.g., "Bitcoin Up or Down - December 2, 1PM ET")
    icon: str = ""           # Icon URL for the coin

    def usdc_value(self) -> float:
        """Calculate USDC value of this trade."""
        return self.size * self.price
    
    def get_coin_name(self) -> str:
        """Extract coin name from title (e.g., 'Bitcoin', 'Ethereum', 'Solana')."""
        if not self.title:
            return "Unknown"
        # Extract first word before "Up or Down"
        parts = self.title.split(" Up or Down")
        if parts:
            return parts[0].strip()
        return "Unknown"
    
    def get_price_change_display(self) -> str:
        """Get price change in cents format (e.g., 'Up 79¢' or 'Down 24¢')."""
        cents = int(self.price * 100)
        if self.outcome.upper() == "UP":
            return f"Up {cents}¢"
        elif self.outcome.upper() == "DOWN":
            return f"Down {cents}¢"
        else:
            return f"{self.outcome} {cents}¢"

    def __str__(self) -> str:
        dt = datetime.fromtimestamp(self.timestamp)
        return (f"Trade(tx={self.transaction_hash[:10]}..., {self.side} "
                f"{self.outcome}, size={self.size:.2f}, price={self.price:.4f}, "
                f"value=${self.usdc_value():.2f}, time={dt.strftime('%H:%M:%S')})")


@dataclass
class BotState:
    """Persistent state to prevent duplicate trade processing."""
    last_seen_timestamp: int
    seen_transaction_hashes: set[str]
    open_positions: Dict[str, Dict]
    closed_positions: List[Dict]
    realized_pnl: float

    def save(self, filepath: Path) -> None:
        """Save state to JSON file."""
        try:
            data = {
                "last_seen_timestamp": self.last_seen_timestamp,
                "seen_transaction_hashes": list(self.seen_transaction_hashes),
                "open_positions": self.open_positions,
                "closed_positions": self.closed_positions,
                "realized_pnl": self.realized_pnl,
            }
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2)
            logger.debug(f"State saved: {len(self.seen_transaction_hashes)} transactions tracked")
        except Exception as e:
            logger.error(f"Failed to save state: {e}")

    @staticmethod
    def load(filepath: Path) -> 'BotState':
        """Load state from JSON file, or create fresh state if not found."""
        if filepath.exists():
            try:
                with open(filepath, 'r') as f:
                    data = json.load(f)
                state = BotState(
                    last_seen_timestamp=data.get("last_seen_timestamp", int(time.time())),
                    seen_transaction_hashes=set(data.get("seen_transaction_hashes", [])),
                    open_positions=data.get("open_positions", {}),
                    closed_positions=data.get("closed_positions", []),
                    realized_pnl=float(data.get("realized_pnl", 0.0)),
                )
                logger.info(f"Loaded state: {len(state.seen_transaction_hashes)} transactions tracked")
                return state
            except Exception as e:
                logger.warning(f"Failed to load state: {e}. Starting fresh.")

        logger.info("Starting with fresh state")
        # Look back 1 hour when starting fresh to catch recent trades
        initial_timestamp = int(time.time()) - (60 * 60)
        return BotState(
            last_seen_timestamp=initial_timestamp,
            seen_transaction_hashes=set(),
            open_positions={},
            closed_positions=[],
            realized_pnl=0.0
        )


POSITION_EPSILON = 1e-6
MAX_CLOSED_POSITIONS = 200


@dataclass
class PositionEvent:
    """Represents a position lifecycle event for notifications."""
    event_type: str
    message: str
    payload: Optional[Dict] = None
    realized_pnl: float = 0.0


class PositionTracker:
    """Tracks open positions, closed trades, and realized PnL."""

    def __init__(
        self,
        open_positions: Optional[Dict[str, Dict]] = None,
        closed_positions: Optional[List[Dict]] = None,
        realized_pnl: float = 0.0,
        trade_history: Optional[List[Dict]] = None
    ) -> None:
        self.open_positions: Dict[str, Dict] = open_positions or {}
        self.closed_positions: List[Dict] = closed_positions or []
        self.realized_pnl: float = realized_pnl
        self.trade_history: List[Dict] = trade_history or []  # Track all trades for volume calculation
        self._lock = threading.RLock()
    
    def save_to_file(self, filepath: Path) -> None:
        """Save tracker state to JSON file."""
        try:
            with self._lock:
                # Recalculate PnL from closed positions before saving to ensure accuracy
                recalculated_pnl = sum(float(entry.get("realized_pnl", 0.0)) for entry in self.closed_positions)
                if abs(recalculated_pnl - self.realized_pnl) > 0.01:
                    logger.info(f"Correcting PnL before save: {self.realized_pnl:.2f} -> {recalculated_pnl:.2f}")
                    self.realized_pnl = recalculated_pnl
                
                data = {
                    "open_positions": self.open_positions,
                    "closed_positions": self.closed_positions,
                    "realized_pnl": self.realized_pnl,
                    "trade_history": self.trade_history
                }
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2)
            logger.debug(f"User state saved to {filepath}")
        except Exception as e:
            logger.error(f"Failed to save user state to {filepath}: {e}")
    
    @staticmethod
    def load_from_file(filepath: Path) -> 'PositionTracker':
        """Load tracker state from JSON file, or create fresh if not found."""
        if filepath.exists():
            try:
                with open(filepath, 'r') as f:
                    data = json.load(f)
                tracker = PositionTracker(
                    open_positions=data.get("open_positions", {}),
                    closed_positions=data.get("closed_positions", []),
                    realized_pnl=float(data.get("realized_pnl", 0.0)),
                    trade_history=data.get("trade_history", [])
                )
                logger.info(f"Loaded user state from {filepath}: {len(tracker.open_positions)} open positions, {len(tracker.closed_positions)} closed")
                return tracker
            except Exception as e:
                logger.warning(f"Failed to load user state from {filepath}: {e}. Starting fresh.")
        
        return PositionTracker()

    def apply_trade(self, trade: Trade, my_size: float) -> List[PositionEvent]:
        """
        Update tracked positions with a newly copied trade.
        Returns list of events describing what changed.
        
        In prediction markets, buying opposite outcomes on the same market
        should be treated as closing/hedging the position.
        """
        if my_size <= 0:
            return []

        signed_size = my_size if trade.side == "BUY" else -my_size
        if abs(signed_size) < POSITION_EPSILON:
            return []

        events: List[PositionEvent] = []
        timestamp = trade.timestamp or int(time.time())
        
        with self._lock:
            # Record trade in history for volume tracking
            trade_record = {
                "timestamp": timestamp,
                "transaction_hash": trade.transaction_hash,
                "market_id": trade.market_id,
                "outcome": trade.outcome,
                "side": trade.side,
                "size": trade.size,
                "price": trade.price,
                "usdc_value": trade.usdc_value(),
                "my_size": my_size,
                "my_usdc_value": my_size * trade.price,
                "title": trade.title,
                "coin_name": trade.get_coin_name()
            }
            self.trade_history.append(trade_record)
            # Keep only last 1000 trades to prevent memory issues
            if len(self.trade_history) > 1000:
                self.trade_history = self.trade_history[-1000:]
            # First, check if we have an opposite position on the same market
            # In prediction markets, buying opposite outcomes closes the position
            # BUT: only hedge if we have an open position in the opposite direction
            opposite_outcome = "Down" if trade.outcome.upper() == "UP" else "Up"
            opposite_key = None
            for key, pos in self.open_positions.items():
                if (pos.get("market_id") == trade.market_id and 
                    pos.get("outcome") == opposite_outcome):
                    # Only match if the position is actually open (non-zero size)
                    pos_size = pos.get("net_size", 0.0)
                    if abs(pos_size) >= POSITION_EPSILON:
                        opposite_key = key
                        break
            
            # If we have an opposite position, treat this as closing/hedging
            if opposite_key:
                opposite_pos = self.open_positions[opposite_key]
                opposite_size = opposite_pos.get("net_size", 0.0)
                
                # In prediction markets, hedging happens when:
                # - You have a long position (positive size) in one outcome
                # - And you BUY the opposite outcome (which creates a long position in the opposite)
                # This locks in a guaranteed payout
                #
                # We only hedge if:
                # 1. We have a long position (opposite_size > 0) in the opposite outcome
                # 2. And we're BUYING (trade.side == "BUY") the current outcome
                # This means we're buying both sides, which hedges the position
                if trade.side == "BUY" and opposite_size > 0:
                    # Valid hedge: long position in opposite, buying current outcome
                    pass  # Continue with hedging
                else:
                    # Not a valid hedge - treat as new position
                    opposite_key = None
                
                if opposite_key:
                    # Calculate closing size
                    closing_size = min(abs(opposite_size), abs(signed_size))
                    
                    # In prediction markets: if you buy Up at $0.60 and Down at $0.40,
                    # you've locked in a loss of $0.20 per share (the spread)
                    # PnL = -closing_size * (entry_price + exit_price - 1.0)
                    # Because: you pay entry + exit, but can only win $1.00
                    entry_price = opposite_pos.get("avg_entry_price", 0.0)
                    exit_price = trade.price
                    
                    # For prediction markets: when you hedge, you're locking in a guaranteed payout
                    # If you buy Up at $0.60 and Down at $0.40 for the same size:
                    # - You paid: $0.60 + $0.40 = $1.00 per share
                    # - You're guaranteed: $1.00 per share (one will win)
                    # - Net: $0.00 (break even)
                    # 
                    # If prices moved (e.g., Up at $0.60, Down at $0.50):
                    # - You paid: $0.60 + $0.50 = $1.10 per share
                    # - You're guaranteed: $1.00 per share
                    # - Net: -$0.10 per share (loss)
                    #
                    # PnL = closing_size * (1.0 - entry_price - exit_price)
                    # This gives: positive when you profit, negative when you lose
                    pnl = closing_size * (1.0 - entry_price - exit_price)
                    self.realized_pnl += pnl
                    
                    # Update remaining sizes
                    # If opposite_size is positive (long), subtract closing_size
                    # If opposite_size is negative (short), add closing_size
                    if opposite_size > 0:
                        remaining_opposite = opposite_size - closing_size
                    else:
                        remaining_opposite = opposite_size + closing_size
                    
                    # If signed_size is positive (buying), subtract closing_size
                    # If signed_size is negative (selling), add closing_size
                    if signed_size > 0:
                        remaining_new = signed_size - closing_size
                    else:
                        remaining_new = signed_size + closing_size
                    
                    # Record closed position
                    closed_entry = {
                        "token_id": opposite_pos.get("token_id", ""),
                        "market_id": trade.market_id,
                        "outcome": f"{opposite_outcome} → {trade.outcome}",
                        "size": closing_size,
                        "entry_price": entry_price,
                        "exit_price": exit_price,
                        "realized_pnl": pnl,
                        "closed_at": timestamp,
                        "type": "HEDGE_CLOSE" if abs(remaining_new) < POSITION_EPSILON else "PARTIAL_HEDGE",
                        "title": trade.title or opposite_pos.get("title", ""),
                        "coin_name": trade.get_coin_name() or opposite_pos.get("coin_name", "Unknown")
                    }
                    self.closed_positions.append(closed_entry)
                    if len(self.closed_positions) > MAX_CLOSED_POSITIONS:
                        self.closed_positions = self.closed_positions[-MAX_CLOSED_POSITIONS:]
                    
                    # Update or remove opposite position
                    if abs(remaining_opposite) < POSITION_EPSILON:
                        if opposite_key is not None:
                            self.open_positions.pop(opposite_key, None)
                    else:
                        opposite_pos["net_size"] = remaining_opposite
                        opposite_pos["last_update"] = timestamp
                    
                    # Create new position if remaining (but check if one already exists)
                    if abs(remaining_new) >= POSITION_EPSILON:
                        # Check if we already have a position for this token_id
                        existing_new = self.open_positions.get(trade.token_id)
                        if existing_new:
                            # Add to existing position
                            existing_size = existing_new.get("net_size", 0.0)
                            total_size = existing_size + remaining_new
                            if abs(total_size) < POSITION_EPSILON:
                                self.open_positions.pop(trade.token_id, None)
                            else:
                                # Recalculate average price
                                total_abs = abs(existing_size) + abs(remaining_new)
                                new_avg = (
                                    abs(existing_size) * existing_new.get("avg_entry_price", 0.0) +
                                    abs(remaining_new) * trade.price
                                ) / total_abs
                                existing_new["net_size"] = total_size
                                existing_new["avg_entry_price"] = new_avg
                                existing_new["last_update"] = timestamp
                        else:
                            # Create new position
                            new_position = {
                                "token_id": trade.token_id,
                                "market_id": trade.market_id,
                                "outcome": trade.outcome or "Unknown",
                                "net_size": remaining_new,
                                "avg_entry_price": trade.price,
                                "last_update": timestamp,
                                "title": trade.title or "",
                                "coin_name": trade.get_coin_name()
                            }
                            self.open_positions[trade.token_id] = new_position
                    
                    message = (f"Hedged/Closed {closing_size:.2f} {opposite_outcome} position "
                              f"by buying {closing_size:.2f} {trade.outcome}. "
                              f"Realized PnL: ${pnl:.2f}")
                    
                    events.append(PositionEvent(
                        event_type=closed_entry["type"],
                        message=message,
                        payload=closed_entry,
                        realized_pnl=pnl
                    ))
                    return events
            
            # No opposite position, proceed with normal tracking
            existing = self.open_positions.get(trade.token_id)

            # If no existing position, create one
            if not existing or abs(existing.get("net_size", 0.0)) < POSITION_EPSILON:
                new_position = {
                    "token_id": trade.token_id,
                    "market_id": trade.market_id,
                    "outcome": trade.outcome or "Unknown",
                    "net_size": signed_size,
                    "avg_entry_price": trade.price,
                    "last_update": timestamp,
                    "title": trade.title or "",
                    "coin_name": trade.get_coin_name()
                }
                self.open_positions[trade.token_id] = new_position
                direction = "long" if signed_size > 0 else "short"
                events.append(PositionEvent(
                    event_type="OPENED",
                    message=(f"Opened {direction} {abs(signed_size):.2f} {trade.outcome} "
                             f"@ ${trade.price:.4f}"),
                    payload=new_position.copy()
                ))
                return events

            current_size = existing["net_size"]
            # Same direction -> increase
            if current_size * signed_size > 0:
                total_abs = abs(current_size) + abs(signed_size)
                new_avg = (
                    abs(current_size) * existing["avg_entry_price"] +
                    abs(signed_size) * trade.price
                ) / total_abs

                existing["net_size"] = current_size + signed_size
                existing["avg_entry_price"] = new_avg
                existing["last_update"] = timestamp
                direction = "long" if existing["net_size"] > 0 else "short"
                events.append(PositionEvent(
                    event_type="INCREASED",
                    message=(f"Increased {direction} {trade.outcome} to "
                             f"{abs(existing['net_size']):.2f} @ ${existing['avg_entry_price']:.4f}"),
                    payload=existing.copy()
                ))
                return events

            # Opposite direction -> closing/reversing
            closing_size = min(abs(current_size), abs(signed_size))
            pnl = closing_size * (trade.price - existing["avg_entry_price"]) * (
                1 if current_size > 0 else -1
            )
            self.realized_pnl += pnl

            remaining = current_size + signed_size
            close_type = "FULL_CLOSE" if abs(remaining) < POSITION_EPSILON else "PARTIAL_CLOSE"
            direction = "long" if current_size > 0 else "short"

            closed_entry = {
                "token_id": trade.token_id,
                "market_id": trade.market_id,
                "outcome": trade.outcome or "Unknown",
                "size": closing_size,
                "entry_price": existing["avg_entry_price"],
                "exit_price": trade.price,
                "realized_pnl": pnl,
                "closed_at": timestamp,
                "type": close_type,
                "title": trade.title or existing.get("title", ""),
                "coin_name": trade.get_coin_name() or existing.get("coin_name", "Unknown")
            }
            self.closed_positions.append(closed_entry)
            if len(self.closed_positions) > MAX_CLOSED_POSITIONS:
                self.closed_positions = self.closed_positions[-MAX_CLOSED_POSITIONS:]

            message = (f"Closed {closing_size:.2f} {direction} {trade.outcome} "
                       f"@ ${trade.price:.4f}. Realized PnL: ${pnl:.2f}.")

            if abs(remaining) < POSITION_EPSILON:
                message += " Position fully closed."
                self.open_positions.pop(trade.token_id, None)
            elif (remaining > 0 and current_size < 0) or (remaining < 0 and current_size > 0):
                # Position flipped to opposite direction
                new_position = {
                    "token_id": trade.token_id,
                    "market_id": trade.market_id,
                    "outcome": trade.outcome or "Unknown",
                    "net_size": remaining,
                    "avg_entry_price": trade.price,
                    "last_update": timestamp,
                    "title": trade.title or existing.get("title", ""),
                    "coin_name": trade.get_coin_name() or existing.get("coin_name", "Unknown")
                }
                self.open_positions[trade.token_id] = new_position
                direction = "long" if remaining > 0 else "short"
                message += (f" Reversed to {direction} {abs(remaining):.2f} "
                            f"@ ${trade.price:.4f}.")
            else:
                existing["net_size"] = remaining
                existing["last_update"] = timestamp
                direction = "long" if remaining > 0 else "short"
                message += (f" Remaining {direction} size: "
                            f"{abs(remaining):.2f} @ ${existing['avg_entry_price']:.4f}.")

            events.append(PositionEvent(
                event_type=close_type,
                message=message,
                payload=closed_entry,
                realized_pnl=pnl
            ))
            return events

    def snapshot_open_positions(self) -> List[Dict]:
        """Return a copy of current open positions."""
        with self._lock:
            return [pos.copy() for pos in self.open_positions.values()]

    def snapshot_closed_positions(self, limit: int = 5) -> List[Dict]:
        """Return the most recent closed position records."""
        with self._lock:
            if limit <= 0:
                return [entry.copy() for entry in self.closed_positions]
            return [entry.copy() for entry in self.closed_positions[-limit:]]

    def serialize_open_positions(self) -> Dict[str, Dict]:
        """Return snapshot formatted for persistence."""
        with self._lock:
            return {token_id: pos.copy() for token_id, pos in self.open_positions.items()}

    def serialize_closed_positions(self) -> List[Dict]:
        """Return closed position log for persistence."""
        with self._lock:
            return [entry.copy() for entry in self.closed_positions]

    def total_realized_pnl(self) -> float:
        """Get total realized PnL. Also recalculate from closed positions to ensure accuracy."""
        with self._lock:
            # Recalculate from closed positions to ensure accuracy
            recalculated = sum(float(entry.get("realized_pnl", 0.0)) for entry in self.closed_positions)
            # Use the stored value, but log if there's a discrepancy
            if abs(recalculated - self.realized_pnl) > 0.01:
                logger.warning(f"PnL discrepancy detected: stored={self.realized_pnl:.2f}, recalculated={recalculated:.2f}. Using recalculated value.")
                self.realized_pnl = recalculated
            return self.realized_pnl
    
    def get_hourly_stats(self, start_timestamp: int, end_timestamp: int) -> Dict:
        """Get statistics for a time period: volume, max trade, peak concurrent exposure, PnL."""
        with self._lock:
            # Get trades in the period, sorted by timestamp
            period_trades = [
                t for t in self.trade_history
                if start_timestamp <= t.get("timestamp", 0) < end_timestamp
            ]
            period_trades.sort(key=lambda t: t.get("timestamp", 0))
            
            # Calculate volume (total USDC value of all trades)
            volume = sum(float(t.get("my_usdc_value", 0.0)) for t in period_trades)
            
            # Find max single trade
            max_trade = None
            max_value = 0.0
            for t in period_trades:
                value = float(t.get("my_usdc_value", 0.0))
                if value > max_value:
                    max_value = value
                    max_trade = t
            
            # Calculate peak concurrent exposure (most money actively being used at once)
            # We need to simulate the state of open positions over time
            peak_concurrent_exposure = 0.0
            
            # Track open positions as we process trades chronologically
            # Key: token_id, Value: {"size": float, "avg_price": float, "value": float}
            active_positions: Dict[str, Dict] = {}
            
            # Start with positions that were open at the start of the hour
            # (positions that were opened before start_timestamp but not closed before it)
            for entry in self.closed_positions:
                opened_at = entry.get("opened_at", 0)
                closed_at = entry.get("closed_at", 0)
                # If position was opened before hour start and closed during or after hour
                if opened_at < start_timestamp and closed_at >= start_timestamp:
                    token_id = entry.get("token_id", "")
                    if token_id:
                        size = abs(float(entry.get("entry_size", 0.0)))
                        avg_price = float(entry.get("entry_price", 0.0))
                        active_positions[token_id] = {
                            "size": size,
                            "avg_price": avg_price,
                            "value": size * avg_price
                        }
            
            # Also include currently open positions that started before the hour
            for token_id, pos in self.open_positions.items():
                opened_at = pos.get("last_update", 0)  # Use last_update as proxy for opened_at
                if opened_at < start_timestamp:
                    size = abs(float(pos.get("net_size", 0.0)))
                    avg_price = float(pos.get("avg_entry_price", 0.0))
                    active_positions[token_id] = {
                        "size": size,
                        "avg_price": avg_price,
                        "value": size * avg_price
                    }
            
            # Calculate initial concurrent exposure
            current_exposure = sum(pos["value"] for pos in active_positions.values())
            peak_concurrent_exposure = max(peak_concurrent_exposure, current_exposure)
            
            # Process trades chronologically to track position changes
            # Use a simplified simulation: track by market_id + outcome (unique position identifier)
            for trade in period_trades:
                market_id = trade.get("market_id", "")
                outcome = trade.get("outcome", "")
                side = trade.get("side", "")
                my_size = float(trade.get("my_size", 0.0))
                price = float(trade.get("price", 0.0))
                my_value = float(trade.get("my_usdc_value", 0.0))
                
                # Create unique key for this position (market + outcome)
                position_key = f"{market_id}:{outcome}"
                
                # Check for opposite position (hedging scenario)
                opposite_outcome = "Down" if outcome.upper() == "UP" else "Up"
                opposite_key = f"{market_id}:{opposite_outcome}"
                
                if side == "BUY":
                    # Check if we have an opposite position (hedging)
                    if opposite_key in active_positions:
                        # Hedging: reduce or close opposite position
                        opposite_pos = active_positions[opposite_key]
                        opposite_size = opposite_pos["size"]
                        opposite_value = opposite_pos["value"]
                        
                        if my_size >= opposite_size:
                            # Opposite position fully closed by hedging
                            active_positions.pop(opposite_key, None)
                            # Remaining size becomes new position
                            remaining = my_size - opposite_size
                            if remaining > 0:
                                remaining_value = remaining * price
                                active_positions[position_key] = {
                                    "size": remaining,
                                    "avg_price": price,
                                    "value": remaining_value
                                }
                        else:
                            # Partial hedge: reduce opposite position
                            reduction_ratio = my_size / opposite_size
                            active_positions[opposite_key]["size"] = opposite_size - my_size
                            active_positions[opposite_key]["value"] = opposite_value * (1 - reduction_ratio)
                    else:
                        # Normal buy: add to or create position
                        if position_key in active_positions:
                            # Add to existing position
                            old_value = active_positions[position_key]["value"]
                            active_positions[position_key]["size"] += my_size
                            active_positions[position_key]["value"] = old_value + my_value
                        else:
                            # New position
                            active_positions[position_key] = {
                                "size": my_size,
                                "avg_price": price,
                                "value": my_value
                            }
                else:  # SELL
                    # Reducing position decreases exposure
                    if position_key in active_positions:
                        current_size = active_positions[position_key]["size"]
                        current_value = active_positions[position_key]["value"]
                        
                        if my_size >= current_size:
                            # Position fully closed
                            active_positions.pop(position_key, None)
                        else:
                            # Partial close - reduce proportionally
                            reduction_ratio = my_size / current_size
                            active_positions[position_key]["value"] = current_value * (1 - reduction_ratio)
                            active_positions[position_key]["size"] = current_size - my_size
                    # If selling without an open position, it doesn't affect exposure
                
                # Update current exposure and track peak
                current_exposure = sum(pos["value"] for pos in active_positions.values())
                peak_concurrent_exposure = max(peak_concurrent_exposure, current_exposure)
            
            # Get PnL for the period
            pnl = sum(
                float(entry.get("realized_pnl", 0.0))
                for entry in self.closed_positions
                if start_timestamp <= entry.get("closed_at", 0) < end_timestamp
            )
            
            return {
                "volume": volume,
                "max_trade": max_trade,
                "max_value": max_value,
                "peak_concurrent_exposure": peak_concurrent_exposure,
                "pnl": pnl,
                "trade_count": len(period_trades)
            }
    
    def realized_pnl_since(self, start_timestamp: int) -> float:
        """Calculate realized PnL from closed positions since a specific timestamp."""
        with self._lock:
            total = 0.0
            for entry in self.closed_positions:
                closed_at = entry.get("closed_at", 0)
                # Handle both timestamp formats (int or float)
                if isinstance(closed_at, float):
                    closed_at = int(closed_at)
                if closed_at >= start_timestamp:
                    pnl = entry.get("realized_pnl", 0.0)
                    # Ensure PnL is a float
                    if isinstance(pnl, (int, float)):
                        total += float(pnl)
            return total


class TelegramNotifier:
    """Sends Telegram notifications and responds to status commands.
    Each user gets their own fresh state when they press /start."""

    def __init__(
        self,
        bot_token: str,
        chat_id: str,
        position_tracker: PositionTracker,  # Legacy: kept for backward compatibility but not used
        target_wallet: str = "0xeffcc79a8572940cee2238b44eac89f2c48fda88"
    ) -> None:
        self.bot_token = bot_token.strip()
        self.chat_id_str = chat_id.strip()
        self.enabled = bool(self.bot_token)
        self.target_wallet = target_wallet
        self.application: Optional[Application] = None
        # Each chat_id gets its own PositionTracker for independent tracking
        self.user_trackers: Dict[int, PositionTracker] = {}
        self.active_chats: set[int] = set()  # Track chats that have used /start
        self._lock = threading.RLock()  # Lock for thread-safe access to user_trackers

        if not self.enabled:
            logger.info("Telegram integration disabled (set TELEGRAM_BOT_TOKEN to enable).")
            return

        try:
            # Build Application (timezone is already patched at module level)
            self.application = Application.builder().token(self.bot_token).build()
            
            # Add handlers
            self.application.add_handler(CommandHandler("start", self._handle_start))
            self.application.add_handler(CommandHandler("help", self._handle_help))
            self.application.add_handler(CommandHandler("status", self._handle_status))
            self.application.add_handler(CommandHandler("openpositions", self._handle_open_positions))
            self.application.add_handler(CommandHandler("closedpositions", self._handle_closed_positions))
            self.application.add_handler(CommandHandler("pnl", self._handle_pnl))
            self.application.add_handler(CommandHandler("pnltoday", self._handle_pnl_today))
            self.application.add_handler(CommandHandler("reset", self._handle_reset))
            self.application.add_handler(CommandHandler("money", self._handle_money))
            
            # Schedule hourly summary job
            # Calculate seconds until next hour
            now = datetime.now()
            next_hour = (now.replace(minute=0, second=0, microsecond=0) + 
                        timedelta(hours=1))
            seconds_until_next_hour = (next_hour - now).total_seconds()
            
            # Schedule recurring job to run at the start of each hour
            job_queue = self.application.job_queue
            if job_queue:
                # Run first summary after current hour ends
                job_queue.run_repeating(
                    self._send_hourly_summary,
                    interval=3600,  # Every hour (3600 seconds)
                    first=seconds_until_next_hour,
                    name="hourly_summary"
                )
                logger.info(f"Scheduled hourly summaries. First summary in {int(seconds_until_next_hour)}s")
            
            # Start polling in background thread
            def start_polling():
                try:
                    if self.application is not None:
                        self.application.run_polling(
                            allowed_updates=Update.ALL_TYPES,
                            drop_pending_updates=True,
                            stop_signals=None  # Don't handle signals in thread
                        )
                except Exception as e:
                    logger.error(f"Error in Telegram polling thread: {e}")
            
            polling_thread = threading.Thread(target=start_polling, daemon=True)
            polling_thread.start()
            logger.info("Telegram bot polling started. Each user gets fresh state on /start.")
        except Exception as exc:
            logger.error(f"Failed to initialize Telegram bot: {exc}", exc_info=True)
            self.enabled = False
            return

    def stop(self) -> None:
        if self.application:
            self.application.stop()  # type: ignore
            self.application.shutdown()  # type: ignore

    def get_user_state_file(self, chat_id: int) -> Path:
        """Get the state file path for a specific user."""
        return Path(f"user_state_{chat_id}.json")
    
    def get_user_tracker(self, chat_id: int) -> PositionTracker:
        """Get or create a PositionTracker for a specific user, loading from file if exists."""
        with self._lock:
            if chat_id not in self.user_trackers:
                # Try to load from file, or create fresh
                state_file = self.get_user_state_file(chat_id)
                self.user_trackers[chat_id] = PositionTracker.load_from_file(state_file)
                logger.info(f"Loaded/created tracker for user chat_id={chat_id}")
            return self.user_trackers[chat_id]
    
    def save_user_tracker(self, chat_id: int) -> None:
        """Save a user's tracker to file."""
        if chat_id in self.user_trackers:
            state_file = self.get_user_state_file(chat_id)
            self.user_trackers[chat_id].save_to_file(state_file)
    
    def reset_user_tracker(self, chat_id: int) -> PositionTracker:
        """Reset a user's tracker to fresh state and save."""
        with self._lock:
            self.user_trackers[chat_id] = PositionTracker()
            state_file = self.get_user_state_file(chat_id)
            # Delete the state file if it exists
            if state_file.exists():
                state_file.unlink()
            logger.info(f"Reset tracker for user chat_id={chat_id}")
            return self.user_trackers[chat_id]

    def notify_events(self, events: List[PositionEvent], chat_id: Optional[int] = None) -> None:
        """Send notifications to a specific user or all active users.
        If chat_id is provided, only send to that user. Otherwise send to all active users."""
        if chat_id is not None:
            # Send to specific user
            for event in events:
                self.send_message(event.message, chat_id=chat_id)
        else:
            # Send to all active users
            for event in events:
                self.send_message_to_all(event.message)

    async def send_message_async(self, text: str, chat_id: int) -> None:
        """Send message to a specific chat (async)."""
        if not self.enabled or not self.application:
            return
        try:
            await self.application.bot.send_message(chat_id=chat_id, text=text)
        except Exception as exc:
            logger.error(f"Failed to send Telegram message: {exc}")

    def send_message(self, text: str, chat_id: Optional[int] = None) -> None:
        """Send message to a specific chat or default chat if specified (sync wrapper)."""
        if not self.enabled or not self.application:
            return
        target_chat = chat_id or (int(self.chat_id_str) if self.chat_id_str else None)
        if target_chat is None:
            return
        # Run async function in thread
        import asyncio
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        loop.run_until_complete(self.send_message_async(text, target_chat))

    def send_message_to_all(self, text: str) -> None:
        """Send message to all active chats that have used /start."""
        if not self.enabled or not self.application:
            return
        import asyncio
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        for chat_id in self.active_chats.copy():  # Use copy to avoid modification during iteration
            try:
                loop.run_until_complete(self.send_message_async(text, chat_id))
            except Exception as exc:
                logger.warning(f"Failed to send message to chat {chat_id}: {exc}")
                # Remove chat if it's no longer valid (user blocked bot, etc.)
                self.active_chats.discard(chat_id)

    async def _handle_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /start command - create fresh state for user and show welcome message."""
        if not update.effective_chat:
            return
        chat_id = update.effective_chat.id
        self.active_chats.add(chat_id)
        
        # Create fresh tracker for this user (resets their state)
        with self._lock:
            self.user_trackers[chat_id] = PositionTracker()
        
        # Get target wallet from instance variable
        target_wallet_short = self.target_wallet[:10] + "..." + self.target_wallet[-8:] if len(self.target_wallet) > 18 else self.target_wallet
        
        welcome_text = (
            "🤖 Welcome to Polymarket Copy Bot!\n\n"
            "✅ Bot Activated!\n"
            "🔍 Scanning targeted account for trades...\n\n"
            f"📊 Target Wallet: `{target_wallet_short}`\n\n"
            "✨ Your account has been initialized with fresh state!\n"
            "You'll receive real-time notifications when trades are detected and copied.\n\n"
            "Available commands:\n"
            "/status - Summary of your bot health\n"
            "/openpositions - List your current open positions\n"
            "/closedpositions - Your recent closed trades with PnL\n"
            "/pnl - PnL for current hour (since 1pm, 2pm, etc.)\n"
            "/pnltoday - PnL for today (since 12:00 AM)\n"
            "/money - Volume and max trade for current hour\n"
            "/backfill - Reset & backfill most recent completed hour\n"
            "/help - Show this help message"
        )
        if update.message:
            await update.message.reply_text(welcome_text, parse_mode='Markdown')
        logger.info(f"New user started with fresh state: chat_id={chat_id}")

    async def _handle_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /help command."""
        if not update.effective_chat:
            return
        chat_id = update.effective_chat.id
        self.active_chats.add(chat_id)  # Register on any command
        help_text = (
            "Polymarket Copy Bot Commands:\n"
            "/status - Summary of bot health\n"
            "/openpositions - List current open positions\n"
            "/closedpositions - Recent closed trades with PnL\n"
            "/pnl - PnL for current hour (since 1pm, 2pm, etc.)\n"
            "/pnltoday - PnL for today (since 12:00 AM)\n"
            "/money - Volume and max trade for current hour\n"
            "/reset - Reset all your positions and trades"
        )
        if update.message:
            await update.message.reply_text(help_text)

    async def _handle_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /status command - show user's own status."""
        if not update.effective_chat:
            return
        chat_id = update.effective_chat.id
        self.active_chats.add(chat_id)
        tracker = self.get_user_tracker(chat_id)
        open_positions = tracker.snapshot_open_positions()
        last_closed = tracker.snapshot_closed_positions(limit=1)
        pnl = tracker.total_realized_pnl()
        status_lines = [
            f"📊 Your Status:",
            f"Open positions: {len(open_positions)}",
            f"Realized PnL: ${pnl:.2f}"
        ]
        if last_closed:
            lc = last_closed[-1]
            status_lines.append(
                f"Last closed: {lc.get('outcome')} {lc.get('type')} "
                f"PnL ${lc.get('realized_pnl', 0.0):.2f}"
            )
        if update.message:
            await update.message.reply_text("\n".join(status_lines))

    async def _handle_open_positions(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /openpositions command - show user's own positions."""
        if not update.effective_chat:
            return
        chat_id = update.effective_chat.id
        self.active_chats.add(chat_id)
        tracker = self.get_user_tracker(chat_id)
        positions = tracker.snapshot_open_positions()
        if not positions:
            if update.message:
                await update.message.reply_text("📭 No open positions.")
            return
        
        lines = ["📈 Your Open Positions:\n"]
        total_value = 0.0
        for pos in positions:
            coin = pos.get("coin_name", "Unknown")
            outcome = pos.get("outcome", "Unknown")
            title = pos.get("title", "")
            size = abs(pos.get("net_size", 0.0))
            price = pos.get("avg_entry_price", 0.0)
            value = size * price
            total_value += value
            
            outcome_emoji = "🟢" if outcome.upper() == "UP" else "🔴"
            price_cents = int(price * 100)
            
            if title:
                lines.append(f"{outcome_emoji} {coin} {outcome} {price_cents}¢")
                lines.append(f"   {title}")
            else:
                lines.append(f"{outcome_emoji} {coin} {outcome} {price_cents}¢")
            
            lines.append(f"   Size: {size:.1f} shares @ ${price:.4f}")
            lines.append(f"   Value: ${value:.2f}\n")
        
        lines.append(f"💰 Total Open Value: ${total_value:.2f}")
        if update.message:
            await update.message.reply_text("\n".join(lines))

    async def _handle_closed_positions(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /closedpositions command - show user's own closed positions."""
        if not update.effective_chat:
            return
        chat_id = update.effective_chat.id
        self.active_chats.add(chat_id)
        tracker = self.get_user_tracker(chat_id)
        closed = tracker.snapshot_closed_positions(limit=10)
        if not closed:
            if update.message:
                await update.message.reply_text("📭 No closed trades yet.")
            return
        
        lines = ["📉 Your Recent Closed Trades:\n"]
        total_pnl = 0.0
        for entry in closed:
            when = datetime.fromtimestamp(entry.get("closed_at", time.time())).strftime("%m-%d %H:%M")
            outcome = entry.get("outcome", "Unknown")
            close_type = entry.get("type", "CLOSE")
            size = entry.get("size", 0.0)
            entry_price = entry.get("entry_price", 0.0)
            exit_price = entry.get("exit_price", 0.0)
            pnl = entry.get("realized_pnl", 0.0)
            total_pnl += pnl
            
            pnl_emoji = "✅" if pnl >= 0 else "❌"
            pnl_sign = "+" if pnl >= 0 else ""
            
            lines.append(f"{when} - {close_type}")
            lines.append(f"   {outcome}")
            lines.append(f"   {size:.1f} shares: ${entry_price:.4f} → ${exit_price:.4f}")
            lines.append(f"   {pnl_emoji} PnL: {pnl_sign}${pnl:.2f}\n")
        
        total_emoji = "✅" if total_pnl >= 0 else "❌"
        total_sign = "+" if total_pnl >= 0 else ""
        lines.append(f"{total_emoji} Total PnL: {total_sign}${total_pnl:.2f}")
        if update.message:
            await update.message.reply_text("\n".join(lines))

    async def _handle_pnl(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /pnl command - show user's PnL for the current hour."""
        if not update.effective_chat:
            return
        chat_id = update.effective_chat.id
        self.active_chats.add(chat_id)
        tracker = self.get_user_tracker(chat_id)
        
        # Calculate start of current hour (e.g., if it's 2:30pm, start from 2:00pm)
        now = datetime.now()
        hour_start = now.replace(minute=0, second=0, microsecond=0)
        start_timestamp = int(hour_start.timestamp())
        
        # Get PnL for this hour
        hourly_pnl = tracker.realized_pnl_since(start_timestamp)
        
        # Get all-time stats
        total_pnl = tracker.total_realized_pnl()
        open_positions = tracker.snapshot_open_positions()
        closed_count = len(tracker.snapshot_closed_positions(limit=0))
        
        # Get closed positions this hour
        closed_this_hour = []
        for entry in tracker.snapshot_closed_positions(limit=0):
            closed_at = entry.get("closed_at", 0)
            if isinstance(closed_at, float):
                closed_at = int(closed_at)
            if closed_at >= start_timestamp:
                closed_this_hour.append(entry)
        
        pnl_emoji = "✅" if hourly_pnl >= 0 else "❌"
        pnl_sign = "+" if hourly_pnl >= 0 else ""
        total_emoji = "✅" if total_pnl >= 0 else "❌"
        total_sign = "+" if total_pnl >= 0 else ""
        
        hour_str = hour_start.strftime("%I:%M %p")
        message = (
            f"💰 Profit & Loss - Current Hour\n"
            f"⏰ Since {hour_str}\n\n"
            f"{pnl_emoji} Hourly PnL: {pnl_sign}${hourly_pnl:.2f}\n"
            f"📊 Trades This Hour: {len(closed_this_hour)}\n\n"
            f"{total_emoji} All-Time PnL: {total_sign}${total_pnl:.2f}\n"
            f"📈 Open Positions: {len(open_positions)}\n"
            f"📉 Total Closed Trades: {closed_count}"
        )
        if update.message:
            await update.message.reply_text(message)
    
    async def _handle_money(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /money command - show volume and max trade for current hour."""
        if not update.effective_chat:
            return
        chat_id = update.effective_chat.id
        self.active_chats.add(chat_id)
        tracker = self.get_user_tracker(chat_id)
        
        # Calculate current hour period
        now = datetime.now()
        hour_start = now.replace(minute=0, second=0, microsecond=0)
        hour_end = hour_start + timedelta(hours=1)
        start_timestamp = int(hour_start.timestamp())
        end_timestamp = int(hour_end.timestamp())
        
        # Get hourly stats
        stats = tracker.get_hourly_stats(start_timestamp, end_timestamp)
        
        hour_str = hour_start.strftime("%I:%M %p")
        message = (
            f"💰 Trading Activity - Current Hour\n"
            f"⏰ {hour_str} - {hour_end.strftime('%I:%M %p')}\n\n"
            f"📊 Volume: ${stats['volume']:.2f}\n"
            f"📈 Trades: {stats['trade_count']}\n"
        )
        
        if stats['max_trade']:
            max_t = stats['max_trade']
            coin = max_t.get("coin_name", "Unknown")
            outcome = max_t.get("outcome", "Unknown")
            message += (
                f"\n🔥 Largest Trade:\n"
                f"   {coin} {outcome}\n"
                f"   ${stats['max_value']:.2f} USDC\n"
                f"   {max_t.get('size', 0):.1f} shares @ ${max_t.get('price', 0):.4f}"
            )
        else:
            message += "\n📭 No trades this hour yet"
        
        if update.message:
            await update.message.reply_text(message)
    
    async def _send_hourly_summary(self, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Send hourly summary to all active users at the end of each hour."""
        if not self.enabled or not self.application:
            return
        
        # Calculate the hour that just ended
        now = datetime.now()
        hour_end = now.replace(minute=0, second=0, microsecond=0)
        hour_start = hour_end - timedelta(hours=1)
        start_timestamp = int(hour_start.timestamp())
        end_timestamp = int(hour_end.timestamp())
        
        # Send summary to all active users
        with self._lock:
            active_chats_copy = list(self.active_chats)
        
        for chat_id in active_chats_copy:
            try:
                tracker = self.get_user_tracker(chat_id)
                stats = tracker.get_hourly_stats(start_timestamp, end_timestamp)
                
                if stats['trade_count'] == 0:
                    # Skip if no trades in this hour
                    continue
                
                pnl_emoji = "✅" if stats['pnl'] >= 0 else "❌"
                pnl_sign = "+" if stats['pnl'] >= 0 else ""
                
                message = (
                    f"📊 Hourly Summary\n"
                    f"⏰ {hour_start.strftime('%I:%M %p')} - {hour_end.strftime('%I:%M %p')}\n\n"
                    f"{pnl_emoji} PnL: {pnl_sign}${stats['pnl']:.2f}\n"
                    f"💰 Volume: ${stats['volume']:.2f}\n"
                    f"💎 Peak Concurrent Exposure: ${stats.get('peak_concurrent_exposure', 0.0):.2f}\n"
                    f"📈 Trades: {stats['trade_count']}\n"
                )
                
                if stats['max_trade']:
                    max_t = stats['max_trade']
                    coin = max_t.get("coin_name", "Unknown")
                    outcome = max_t.get("outcome", "Unknown")
                    message += (
                        f"\n🔥 Largest Single Trade:\n"
                        f"   {coin} {outcome} - ${stats['max_value']:.2f}"
                    )
                
                await self.send_message_async(message, chat_id)
                logger.info(f"Sent hourly summary to chat_id={chat_id}")
            except Exception as e:
                logger.warning(f"Failed to send hourly summary to chat {chat_id}: {e}")
                self.active_chats.discard(chat_id)
    
    async def _handle_backfill(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /backfill command - reset state and simulate copying all trades from the most recent completed hour."""
        if not update.effective_chat:
            return
        chat_id = update.effective_chat.id
        self.active_chats.add(chat_id)
        
        if update.message:
            await update.message.reply_text(
                "🔄 Resetting your state and backfilling most recent completed hour...\n"
                "⏳ This may take a moment..."
            )
        
        # Reset user's tracker first
        self.reset_user_tracker(chat_id)
        
        # Calculate most recent completed hour
        # If it's 2:23 PM, we want 1:00 PM - 2:00 PM (the most recent completed hour)
        now = datetime.now()
        current_hour_start = now.replace(minute=0, second=0, microsecond=0)
        hour_end = current_hour_start  # End of the completed hour
        hour_start = hour_end - timedelta(hours=1)  # Start of the completed hour
        start_timestamp = int(hour_start.timestamp())
        end_timestamp = int(hour_end.timestamp())
        
        if update.message:
            await update.message.reply_text(
                f"📥 Fetching trades from most recent completed hour...\n"
                f"⏰ {hour_start.strftime('%I:%M %p')} - {hour_end.strftime('%I:%M %p')}\n\n"
                f"⏳ Fetching from Polymarket..."
            )
        
        # Fetch all trades from the completed hour
        trades = fetch_trades_in_range(TARGET_WALLET, start_timestamp, end_timestamp)
        
        if not trades:
            if update.message:
                await update.message.reply_text(
                    f"📭 No trades found in the hour {hour_start.strftime('%I:%M %p')} - {hour_end.strftime('%I:%M %p')}.\n"
                    "The target wallet may not have traded during this period."
                )
            return
        
        if update.message:
            await update.message.reply_text(
                f"✅ Found {len(trades)} trades!\n"
                f"🔄 Simulating copy trades (in chronological order)..."
            )
        
        # Get user's tracker (fresh after reset)
        tracker = self.get_user_tracker(chat_id)
        
        # Process each trade in chronological order
        processed_count = 0
        skipped_count = 0
        
        for trade in trades:
            # Check if we should copy this trade
            if not should_copy_trade(trade):
                skipped_count += 1
                continue
            
            # Calculate our size (using current risk settings)
            my_size = compute_my_size(trade, RISK_MULTIPLIER, MAX_TRADE_USDC)
            
            if my_size <= 0:
                skipped_count += 1
                continue
            
            # Apply trade to user's tracker (simulate, don't place real orders)
            position_events = tracker.apply_trade(trade, my_size)
            
            # Notify user of position events
            if position_events:
                self.notify_events(position_events, chat_id=chat_id)
            
            processed_count += 1
        
        # Save user state
        self.save_user_tracker(chat_id)
        
        # Get updated stats for the hour
        stats = tracker.get_hourly_stats(start_timestamp, end_timestamp)
        pnl_emoji = "✅" if stats['pnl'] >= 0 else "❌"
        pnl_sign = "+" if stats['pnl'] >= 0 else ""
        
        # Send summary
        message = (
            f"✅ Backfill Complete!\n\n"
            f"⏰ Hour: {hour_start.strftime('%I:%M %p')} - {hour_end.strftime('%I:%M %p')}\n\n"
            f"🔄 State Reset: Yes\n"
            f"📊 Processed: {processed_count} trades\n"
            f"⏭️  Skipped: {skipped_count} trades\n\n"
            f"{pnl_emoji} Hourly PnL: {pnl_sign}${stats['pnl']:.2f}\n"
            f"💰 Volume: ${stats['volume']:.2f}\n"
            f"💎 Peak Concurrent Exposure: ${stats.get('peak_concurrent_exposure', 0.0):.2f}\n"
            f"📈 Total Trades: {stats['trade_count']}\n"
        )
        
        if stats['max_trade']:
            max_t = stats['max_trade']
            coin = max_t.get("coin_name", "Unknown")
            outcome = max_t.get("outcome", "Unknown")
            message += (
                f"\n🔥 Largest Single Trade:\n"
                f"   {coin} {outcome} - ${stats['max_value']:.2f}"
            )
        
        message += (
            f"\n\n💡 Note: Your state was reset and all trades from the completed hour\n"
            f"have been simulated. No real orders were placed."
        )
        
        if update.message:
            await update.message.reply_text(message)
        
        logger.info(f"Backfilled {processed_count} trades (hour {hour_start.strftime('%I:%M %p')}-{hour_end.strftime('%I:%M %p')}) for user chat_id={chat_id} after reset")
    
    async def _handle_reset(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /reset command - reset user's positions and trades."""
        if not update.effective_chat:
            return
        chat_id = update.effective_chat.id
        self.active_chats.add(chat_id)
        
        # Get current stats before reset
        tracker = self.get_user_tracker(chat_id)
        open_count = len(tracker.snapshot_open_positions())
        closed_count = len(tracker.snapshot_closed_positions(limit=0))
        pnl = tracker.total_realized_pnl()
        
        # Reset the tracker
        self.reset_user_tracker(chat_id)
        
        message = (
            "🔄 Reset Complete!\n\n"
            f"📊 Cleared:\n"
            f"   • {open_count} open positions\n"
            f"   • {closed_count} closed trades\n"
            f"   • ${pnl:.2f} realized PnL\n\n"
            "✨ Your account has been reset to a fresh state.\n"
            "All new trades will be tracked from now on."
        )
        if update.message:
            await update.message.reply_text(message)
    
    async def _handle_pnl_today(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /pnltoday command - show user's PnL for today (starting from 12:00 AM)."""
        if not update.effective_chat:
            return
        chat_id = update.effective_chat.id
        self.active_chats.add(chat_id)
        tracker = self.get_user_tracker(chat_id)
        
        # Calculate start of today (12:00 AM)
        now = datetime.now()
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        start_timestamp = int(today_start.timestamp())
        
        # Get PnL for today
        daily_pnl = tracker.realized_pnl_since(start_timestamp)
        
        # Get all-time stats
        total_pnl = tracker.total_realized_pnl()
        open_positions = tracker.snapshot_open_positions()
        closed_count = len(tracker.snapshot_closed_positions(limit=0))
        
        # Get closed positions today
        closed_today = []
        for entry in tracker.snapshot_closed_positions(limit=0):
            closed_at = entry.get("closed_at", 0)
            if isinstance(closed_at, float):
                closed_at = int(closed_at)
            if closed_at >= start_timestamp:
                closed_today.append(entry)
        
        pnl_emoji = "✅" if daily_pnl >= 0 else "❌"
        pnl_sign = "+" if daily_pnl >= 0 else ""
        total_emoji = "✅" if total_pnl >= 0 else "❌"
        total_sign = "+" if total_pnl >= 0 else ""
        
        today_str = today_start.strftime("%B %d, %Y")
        message = (
            f"💰 Profit & Loss - Today\n"
            f"📅 {today_str} (since 12:00 AM)\n\n"
            f"{pnl_emoji} Daily PnL: {pnl_sign}${daily_pnl:.2f}\n"
            f"📊 Trades Today: {len(closed_today)}\n\n"
            f"{total_emoji} All-Time PnL: {total_sign}${total_pnl:.2f}\n"
            f"📈 Open Positions: {len(open_positions)}\n"
            f"📉 Total Closed Trades: {closed_count}"
        )
        if update.message:
            await update.message.reply_text(message)


# ============================================================================
# CORE FUNCTIONS
# ============================================================================

# Reusable HTTP session for lower latency (connection pooling)
_http_session: Optional[requests.Session] = None

def get_http_session() -> requests.Session:
    """Get or create a persistent HTTP session for connection reuse."""
    global _http_session
    if _http_session is None:
        _http_session = requests.Session()
        _http_session.headers.update({
            'User-Agent': 'PolymarketCopyBot/1.0'
        })
    return _http_session


def fetch_trades_in_range(
    target_wallet: str,
    start_timestamp: int,
    end_timestamp: int
) -> list[Trade]:
    """
    Fetch all trades for the target wallet within a time range.
    
    Args:
        target_wallet: Proxy wallet address to monitor
        start_timestamp: Unix timestamp to fetch trades from (inclusive)
        end_timestamp: Unix timestamp to fetch trades until (exclusive)
    
    Returns:
        List of Trade objects sorted by timestamp (oldest first)
    """
    endpoint = f"{DATA_API_BASE_URL}/activity"
    params = {
        "user": target_wallet.lower(),
        "type": "TRADE",
        "sortBy": "TIMESTAMP",
        "sortDirection": "DESC",
        "limit": 200  # Fetch more to ensure we get all trades in the hour
    }
    
    session = get_http_session()
    retry_delay = INITIAL_RETRY_DELAY
    
    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"Fetching trades from Polymarket API for range {start_timestamp}-{end_timestamp} (attempt {attempt + 1}/{MAX_RETRIES})")
            response = session.get(endpoint, params=params, timeout=10)
            
            if response.status_code == 429:
                logger.warning(f"Rate limit hit. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY)
                continue
            
            response.raise_for_status()
            activities = response.json()
            logger.info(f"API returned {len(activities)} activities, filtering for trades in range")
            
            # Parse trades
            trades = []
            for activity in activities:
                tx_hash = activity.get("transactionHash", "")
                activity_timestamp = activity.get("timestamp", 0)
                
                # Skip if no hash
                if not tx_hash:
                    continue
                
                # Filter by timestamp range
                if activity_timestamp < start_timestamp or activity_timestamp >= end_timestamp:
                    continue
                
                try:
                    proxy_wallet = activity.get("proxyWallet", "").lower()
                    market_id = activity.get("conditionId", activity.get("title", ""))
                    
                    trade = Trade(
                        transaction_hash=tx_hash,
                        timestamp=activity_timestamp,
                        market_id=market_id,
                        token_id=activity.get("asset", ""),
                        outcome=activity.get("outcome", ""),
                        side=activity.get("side", ""),
                        size=float(activity.get("size", 0)),
                        price=float(activity.get("price", 0)),
                        proxy_wallet=proxy_wallet,
                        title=activity.get("title", ""),
                        icon=activity.get("icon", "")
                    )
                    
                    if trade.market_id and trade.token_id:
                        trades.append(trade)
                    else:
                        logger.warning(f"Missing market/token ID: {trade}")
                except (ValueError, KeyError) as e:
                    logger.warning(f"Failed to parse trade activity: {e}")
                    continue
            
            # Sort by timestamp (oldest first) so we process trades in chronological order
            trades.sort(key=lambda t: t.timestamp)
            logger.info(f"Found {len(trades)} trades in the specified time range")
            return trades
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY)
            else:
                logger.error("Max retries reached, giving up")
                return []
    
    return []


def fetch_trades_since(
    target_wallet: str,
    last_timestamp: int,
    seen_hashes: set[str]
) -> list[Trade]:
    """
    Fetch new trades for the target wallet since last_timestamp.

    Uses Polymarket Data API /activity endpoint:
    https://docs.polymarket.com/developers/misc-endpoints/data-api-activity

    Args:
        target_wallet: Proxy wallet address to monitor
        last_timestamp: Unix timestamp to fetch trades after
        seen_hashes: Set of already-seen transaction hashes (for deduplication)

    Returns:
        List of new Trade objects (not in seen_hashes)
    """
    endpoint = f"{DATA_API_BASE_URL}/activity"
    # Note: The 'start' parameter doesn't work reliably, so we fetch recent trades
    # and filter by timestamp in code. Fetch last 100 trades to ensure we don't miss any.
    params = {
        "user": target_wallet.lower(),
        "type": "TRADE",
        "sortBy": "TIMESTAMP",
        "sortDirection": "DESC",
        "limit": 100  # Fetch enough to catch recent activity
    }

    session = get_http_session()
    retry_delay = INITIAL_RETRY_DELAY

    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"Fetching trades from Polymarket API (attempt {attempt + 1}/{MAX_RETRIES})")
            response = session.get(endpoint, params=params, timeout=10)

            # Handle rate limiting
            if response.status_code == 429:
                logger.warning(f"Rate limit hit. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY)
                continue

            response.raise_for_status()
            activities = response.json()
            logger.info(f"API returned {len(activities)} activities, filtering for trades after timestamp {last_timestamp}")

            # Parse trades
            trades = []
            for activity in activities:
                tx_hash = activity.get("transactionHash", "")
                activity_timestamp = activity.get("timestamp", 0)

                # Skip duplicates
                if not tx_hash or tx_hash in seen_hashes:
                    continue
                
                # Filter by timestamp - only process trades after last_seen_timestamp
                if activity_timestamp <= last_timestamp:
                    continue

                try:
                    # API returns 'proxyWallet' not 'user', and 'conditionId' not 'market'
                    proxy_wallet = activity.get("proxyWallet", "").lower()
                    market_id = activity.get("conditionId", activity.get("title", ""))
                    
                    trade = Trade(
                        transaction_hash=tx_hash,
                        timestamp=activity_timestamp,
                        market_id=market_id,
                        token_id=activity.get("asset", ""),
                        outcome=activity.get("outcome", ""),
                        side=activity.get("side", ""),
                        size=float(activity.get("size", 0)),
                        price=float(activity.get("price", 0)),
                        proxy_wallet=proxy_wallet,
                        title=activity.get("title", ""),
                        icon=activity.get("icon", "")
                    )

                    # Validate wallet match
                    if trade.proxy_wallet == target_wallet.lower() and trade.transaction_hash:
                        trades.append(trade)
                        seen_hashes.add(tx_hash)

                except (KeyError, ValueError, TypeError) as e:
                    logger.warning(f"Failed to parse activity: {e}")
                    continue

            if trades:
                logger.info(f"Fetched {len(trades)} new trade(s)")

            return trades

        except requests.exceptions.Timeout:
            logger.warning(f"Request timeout (attempt {attempt + 1}/{MAX_RETRIES})")
            if attempt < MAX_RETRIES - 1:
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY)

        except requests.exceptions.RequestException as e:
            logger.error(f"Request error: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY)

        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            break

    return []


def should_copy_trade(trade: Trade) -> bool:
    """
    Determine if a trade should be copied.

    Future filtering logic can be added here:
    - Min/max trade size
    - Specific markets only
    - Specific outcomes only
    - Time-based filters

    Args:
        trade: The Trade object to evaluate

    Returns:
        True if trade should be copied, False otherwise
    """
    # Basic validation
    if trade.size <= 0 or trade.price <= 0:
        logger.warning(f"Invalid trade size/price: {trade}")
        return False

    if not trade.market_id or not trade.token_id:
        logger.warning(f"Missing market/token ID: {trade}")
        return False

    # All trades pass for now - add custom filters here
    return True


def compute_my_size(trade: Trade, risk_multiplier: float, max_usdc: float) -> float:
    """
    Calculate the size for our copy trade based on risk parameters.

    Args:
        trade: The original trade to copy
        risk_multiplier: Multiplier for position sizing (1.0 = exact copy)
        max_usdc: Maximum USDC value allowed per trade

    Returns:
        Size (number of shares) to trade
    """
    # Apply risk multiplier
    desired_size = trade.size * risk_multiplier
    desired_value = desired_size * trade.price

    # Cap at max USDC if needed
    if desired_value > max_usdc:
        capped_size = max_usdc / trade.price
        logger.info(f"Capping size from {desired_size:.2f} to {capped_size:.2f} "
                   f"(max ${max_usdc:.2f} USDC)")
        return capped_size

    return desired_size


def place_copy_order(
    trade: Trade,
    my_size: float,
    clob_client: ClobClient
) -> Optional[str]:
    """
    Place a copy order on Polymarket via CLOB API.

    Uses py-clob-client to sign and submit orders.
    Docs: https://github.com/Polymarket/py-clob-client

    Args:
        trade: The original trade to copy
        my_size: The size (shares) for our order
        clob_client: Initialized ClobClient instance

    Returns:
        Order ID if successful, None if failed
    """
    try:
        # Map side to CLOB constants
        side = BUY if trade.side == "BUY" else SELL

        # Create order arguments
        # Note: token_id maps to the specific outcome token for this market
        order_args = OrderArgs(
            price=trade.price,
            size=my_size,
            side=side,
            token_id=trade.token_id
        )

        logger.info(f"Placing order: {side} {my_size:.2f} @ ${trade.price:.4f} "
                   f"(${my_size * trade.price:.2f} USDC)")

        # Submit order to CLOB
        response = clob_client.create_order(order_args)
        order_id = response.get("orderID", "unknown")

        logger.info(f"✅ Order placed successfully! Order ID: {order_id}")
        return order_id

    except Exception as e:
        error_msg = str(e).lower()

        # Handle common errors gracefully
        if "insufficient balance" in error_msg or "insufficient funds" in error_msg:
            logger.error(f"❌ Insufficient balance (need ~${my_size * trade.price:.2f} USDC)")
        elif "allowance" in error_msg:
            logger.error(f"❌ Insufficient allowance. Set USDC allowance on Polymarket website.")
        else:
            logger.error(f"❌ Order failed: {e}", exc_info=True)

        return None


def process_trade(
    trade: Trade,
    clob_client: Optional[ClobClient],
    risk_multiplier: float,
    max_usdc: float,
    dry_run: bool,
    position_tracker: PositionTracker,  # Legacy: kept for backward compatibility
    notifier: TelegramNotifier
) -> None:
    """
    Process a single detected trade: validate, size, and place copy order.
    Applies the trade to all active Telegram users' trackers.

    Args:
        trade: The Trade to process
        clob_client: CLOB client for order placement (None in dry run)
        risk_multiplier: Risk multiplier for sizing
        max_usdc: Max USDC per trade
        dry_run: If True, don't place real orders
        position_tracker: Legacy tracker (not used when Telegram is enabled)
        notifier: Telegram notifier instance (manages per-user trackers)
    """
    logger.info("=" * 60)
    logger.info(f"🔔 NEW TRADE DETECTED")
    logger.info(f"   Market: {trade.market_id}")
    logger.info(f"   Outcome: {trade.outcome}")
    logger.info(f"   Side: {trade.side}")
    logger.info(f"   Size: {trade.size:.2f} shares @ ${trade.price:.4f}")
    logger.info(f"   Value: ${trade.usdc_value():.2f} USDC")
    logger.info(f"   Tx: {trade.transaction_hash}")
    logger.info("=" * 60)

    # Check if we should copy this trade
    if not should_copy_trade(trade):
        logger.warning("⚠️  Trade filtered out, not copying")
        return

    # Calculate our size
    my_size = compute_my_size(trade, risk_multiplier, max_usdc)
    my_value = my_size * trade.price

    logger.info(f"Our copy: {my_size:.2f} shares (${my_value:.2f} USDC, "
               f"{risk_multiplier}x multiplier)")

    if my_size <= 0:
        logger.warning("Computed trade size is zero after risk controls; skipping.")
        return

    # Apply trade to all active Telegram users' trackers
    if notifier.enabled:
        with notifier._lock:
            active_user_ids = list(notifier.active_chats)
        
        if active_user_ids:
            # Send trade detection notification to all active users
            coin_name = trade.get_coin_name()
            price_change = trade.get_price_change_display()
            outcome_emoji = "🟢" if trade.outcome.upper() == "UP" else "🔴"
            
            trade_notification = (
                f"🔔 NEW TRADE DETECTED\n\n"
                f"{outcome_emoji} {coin_name} {price_change}\n"
                f"📊 {trade.title}\n\n"
                f"Action: {trade.side}\n"
                f"Shares: {trade.size:.1f}\n"
                f"Price: ${trade.price:.4f}\n"
                f"Value: ${trade.usdc_value():.2f}\n\n"
                f"📋 Our Copy:\n"
                f"   {my_size:.1f} shares @ ${trade.price:.4f}\n"
                f"   Total: ${my_value:.2f} USDC"
            )
            notifier.send_message_to_all(trade_notification)
        
        for chat_id in active_user_ids:
            user_tracker = notifier.get_user_tracker(chat_id)
            position_events = user_tracker.apply_trade(trade, my_size)
            # Send position event notifications to this specific user
            notifier.notify_events(position_events, chat_id=chat_id)
            # Save user state after applying trade
            notifier.save_user_tracker(chat_id)
            logger.debug(f"Applied trade to user chat_id={chat_id}")
    
    # Also apply to legacy tracker if Telegram is disabled
    if not notifier.enabled:
        position_events = position_tracker.apply_trade(trade, my_size)
        notifier.notify_events(position_events)

    # Dry run mode - don't place real orders
    if dry_run:
        logger.info("🏃 DRY RUN MODE: Not placing real order")
        return

    # Place the copy order (only once, not per user)
    if clob_client is None:
        logger.error("❌ CLOB client not initialized, cannot place order")
        return

    order_id = place_copy_order(trade, my_size, clob_client)

    if order_id:
        logger.info(f"✅ Successfully copied trade! Order ID: {order_id}")
    else:
        logger.warning(f"⚠️  Failed to copy trade")


def main_loop(
    target_wallet: str,
    clob_client: Optional[ClobClient],
    state: BotState,
    poll_interval: float,
    risk_multiplier: float,
    max_usdc: float,
    dry_run: bool,
    position_tracker: PositionTracker,
    notifier: TelegramNotifier
) -> None:
    """
    Main polling loop: fetch trades, process them, save state.

    Args:
        target_wallet: Wallet address to monitor
        clob_client: CLOB client for placing orders (None in dry run)
        state: BotState for persistence
        poll_interval: Seconds between polls
        risk_multiplier: Position sizing multiplier
        max_usdc: Max USDC per trade
        dry_run: If True, don't place real orders
        position_tracker: Tracker for open/closed positions
        notifier: Telegram notifier instance
    """
    logger.info("Starting monitoring loop... Press Ctrl+C to stop")
    logger.info("")

    try:
        while True:
            # Fetch new trades
            trades = fetch_trades_since(
                target_wallet,
                state.last_seen_timestamp,
                state.seen_transaction_hashes
            )

            # Process each trade immediately (minimize latency)
            for trade in trades:
                process_trade(
                    trade,
                    clob_client,
                    risk_multiplier,
                    max_usdc,
                    dry_run,
                    position_tracker,
                    notifier
                )

                # Update state timestamp
                if trade.timestamp > state.last_seen_timestamp:
                    state.last_seen_timestamp = trade.timestamp

                # Save state after each trade
                # Note: When Telegram is enabled, positions are tracked per-user in-memory
                # Only save global state (seen trades, timestamps)
                if not notifier.enabled:
                    # Legacy mode: save positions to global state
                    state.open_positions = position_tracker.serialize_open_positions()
                    state.closed_positions = position_tracker.serialize_closed_positions()
                    state.realized_pnl = position_tracker.total_realized_pnl()
                else:
                    # Telegram mode: positions are per-user, only save global tracking state
                    state.open_positions = {}
                    state.closed_positions = []
                    state.realized_pnl = 0.0
                state.save(STATE_FILE)

            # Wait before next poll
            time.sleep(poll_interval)

    except KeyboardInterrupt:
        logger.info("\n" + "=" * 60)
        logger.info("Bot stopped by user")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Fatal error in main loop: {e}", exc_info=True)

    finally:
        # Final state save
        if not notifier.enabled:
            # Legacy mode: save positions to global state
            state.open_positions = position_tracker.serialize_open_positions()
            state.closed_positions = position_tracker.serialize_closed_positions()
            state.realized_pnl = position_tracker.total_realized_pnl()
        else:
            # Telegram mode: positions are per-user, only save global tracking state
            state.open_positions = {}
            state.closed_positions = []
            state.realized_pnl = 0.0
        state.save(STATE_FILE)


def print_banner(
    target_wallet: str,
    max_usdc: float,
    risk_multiplier: float,
    poll_interval: float,
    dry_run: bool,
    state: BotState,
    telegram_enabled: bool
) -> None:
    """Print startup banner with configuration."""
    logger.info("")
    logger.info("=" * 60)
    logger.info("   POLYMARKET COPY TRADING BOT")
    logger.info("=" * 60)
    logger.info("")
    logger.info("⚠️  FINANCIAL RISK WARNING ⚠️")
    logger.info("This bot trades with real money. Only use funds you can afford to lose!")
    logger.info("")
    logger.info("Configuration:")
    logger.info(f"  Target Wallet: {target_wallet}")
    logger.info(f"  Max Trade Size: ${max_usdc:.2f} USDC")
    logger.info(f"  Risk Multiplier: {risk_multiplier}x")
    logger.info(f"  Poll Interval: {poll_interval}s")
    logger.info(f"  Dry Run Mode: {dry_run}")
    logger.info(f"  Log Level: {LOG_LEVEL}")
    logger.info(f"  Telegram Alerts Enabled: {telegram_enabled}")
    if telegram_enabled:
        logger.info(f"  Telegram Mode: Public (any user can use /start)")
    logger.info("")
    logger.info("State:")
    logger.info(f"  Tracked transactions: {len(state.seen_transaction_hashes)}")
    logger.info(f"  Last seen: {datetime.fromtimestamp(state.last_seen_timestamp)}")
    logger.info(f"  Open positions: {len(state.open_positions)}")
    logger.info(f"  Realized PnL: ${state.realized_pnl:.2f}")
    logger.info("")


# ============================================================================
# ENTRY POINT
# ============================================================================

def main() -> None:
    """
    Main entry point: parse config, initialize clients, start loop.
    """
    # Validate required configuration
    if not DRY_RUN and not POLYMARKET_PRIVATE_KEY:
        logger.error("")
        logger.error("❌ ERROR: POLYMARKET_PRIVATE_KEY environment variable is required!")
        logger.error("Please set it in your .env file.")
        logger.error("See .env.example for template.")
        logger.error("")
        sys.exit(1)

    # Load persistent state
    state = BotState.load(STATE_FILE)
    position_tracker = PositionTracker(
        open_positions=state.open_positions,
        closed_positions=state.closed_positions,
        realized_pnl=state.realized_pnl
    )
    notifier = TelegramNotifier(
        bot_token=TELEGRAM_BOT_TOKEN,
        chat_id=TELEGRAM_CHAT_ID,
        position_tracker=position_tracker,
        target_wallet=TARGET_WALLET
    )

    # Initialize CLOB client (only if not dry run)
    clob_client: Optional[ClobClient] = None
    if not DRY_RUN:
        try:
            # Remove 0x prefix if present
            private_key = POLYMARKET_PRIVATE_KEY
            if private_key.startswith("0x"):
                private_key = private_key[2:]

            # Initialize client
            # chain_id=137 is Polygon mainnet (where Polymarket operates)
            clob_client = ClobClient(
                host=CLOB_API_BASE_URL,
                key=private_key,
                chain_id=137
            )
            logger.info("Initialized Polymarket CLOB client")

        except Exception as e:
            logger.error(f"Failed to initialize CLOB client: {e}")
            sys.exit(1)

    # Print startup banner
    print_banner(
        TARGET_WALLET,
        MAX_TRADE_USDC,
        RISK_MULTIPLIER,
        POLL_INTERVAL_SECONDS,
        DRY_RUN,
        state,
        notifier.enabled
    )

    # Start main loop
    main_loop(
        target_wallet=TARGET_WALLET,
        clob_client=clob_client,
        state=state,
        poll_interval=POLL_INTERVAL_SECONDS,
        risk_multiplier=RISK_MULTIPLIER,
        max_usdc=MAX_TRADE_USDC,
        dry_run=DRY_RUN,
        position_tracker=position_tracker,
        notifier=notifier
    )

    notifier.stop()


if __name__ == "__main__":
    main()
