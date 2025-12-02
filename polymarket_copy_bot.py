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
from datetime import datetime
from pathlib import Path

# Standard library + requests for HTTP
import requests
from dotenv import load_dotenv

# Polymarket CLOB client for order placement
try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import OrderArgs
    from py_clob_client.order_builder.constants import BUY, SELL
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
    outcome: str             # Human-readable outcome (e.g., "Yes" or "No")
    side: str                # "BUY" or "SELL"
    size: float              # Number of shares
    price: float             # Price per share (0-1 range typically)
    proxy_wallet: str

    def usdc_value(self) -> float:
        """Calculate USDC value of this trade."""
        return self.size * self.price

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
        return BotState(
            last_seen_timestamp=int(time.time()),
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
        realized_pnl: float = 0.0
    ) -> None:
        self.open_positions: Dict[str, Dict] = open_positions or {}
        self.closed_positions: List[Dict] = closed_positions or []
        self.realized_pnl: float = realized_pnl
        self._lock = threading.RLock()

    def apply_trade(self, trade: Trade, my_size: float) -> List[PositionEvent]:
        """
        Update tracked positions with a newly copied trade.
        Returns list of events describing what changed.
        """
        if my_size <= 0:
            return []

        signed_size = my_size if trade.side == "BUY" else -my_size
        if abs(signed_size) < POSITION_EPSILON:
            return []

        events: List[PositionEvent] = []
        timestamp = trade.timestamp or int(time.time())

        with self._lock:
            existing = self.open_positions.get(trade.token_id)

            # If no existing position, create one
            if not existing or abs(existing.get("net_size", 0.0)) < POSITION_EPSILON:
                new_position = {
                    "token_id": trade.token_id,
                    "market_id": trade.market_id,
                    "outcome": trade.outcome or "Unknown",
                    "net_size": signed_size,
                    "avg_entry_price": trade.price,
                    "last_update": timestamp
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
                "type": close_type
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
                    "last_update": timestamp
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
        with self._lock:
            return self.realized_pnl


class TelegramNotifier:
    """Sends Telegram notifications and responds to status commands.
    Each user gets their own fresh state when they press /start."""

    def __init__(
        self,
        bot_token: str,
        chat_id: str,
        position_tracker: PositionTracker  # Legacy: kept for backward compatibility but not used
    ) -> None:
        self.bot_token = bot_token.strip()
        self.chat_id_str = chat_id.strip()
        self.enabled = bool(self.bot_token)
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
            
            # Start polling in background thread
            def start_polling():
                try:
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
            self.application.stop()
            self.application.shutdown()

    def get_user_tracker(self, chat_id: int) -> PositionTracker:
        """Get or create a PositionTracker for a specific user."""
        with self._lock:
            if chat_id not in self.user_trackers:
                # Create fresh tracker for new user
                self.user_trackers[chat_id] = PositionTracker()
                logger.info(f"Created fresh tracker for user chat_id={chat_id}")
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
        
        welcome_text = (
            "🤖 Welcome to Polymarket Copy Bot!\n\n"
            "✨ Your account has been initialized with fresh state!\n"
            "You'll now receive notifications about trades copied from the target wallet.\n\n"
            "Available commands:\n"
            "/status - Summary of your bot health\n"
            "/openpositions - List your current open positions\n"
            "/closedpositions - Your recent closed trades with PnL\n"
            "/pnl - Your realized profit & loss summary\n"
            "/help - Show this help message"
        )
        await update.message.reply_text(welcome_text)
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
            "/pnl - Realized profit & loss summary"
        )
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
            await update.message.reply_text("No open positions.")
            return
        lines = ["📈 Your Open Positions:"]
        for pos in positions:
            direction = "Long" if pos["net_size"] > 0 else "Short"
            lines.append(
                f"{pos['market_id']} ({pos['outcome']}): {direction} "
                f"{abs(pos['net_size']):.2f} @ ${pos['avg_entry_price']:.4f}"
            )
        await update.message.reply_text("\n".join(lines))

    async def _handle_closed_positions(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /closedpositions command - show user's own closed positions."""
        if not update.effective_chat:
            return
        chat_id = update.effective_chat.id
        self.active_chats.add(chat_id)
        tracker = self.get_user_tracker(chat_id)
        closed = tracker.snapshot_closed_positions(limit=5)
        if not closed:
            await update.message.reply_text("No closed trades yet.")
            return
        lines = ["📉 Your Recent Closed Trades:"]
        for entry in closed:
            when = datetime.fromtimestamp(entry.get("closed_at", time.time())).strftime("%m-%d %H:%M")
            lines.append(
                f"{when} - {entry.get('outcome')} {entry.get('type')} "
                f"{entry.get('size', 0):.2f} @ ${entry.get('exit_price', 0):.4f} "
                f"PnL ${entry.get('realized_pnl', 0.0):.2f}"
            )
        await update.message.reply_text("\n".join(lines))

    async def _handle_pnl(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /pnl command - show user's own PnL."""
        if not update.effective_chat:
            return
        chat_id = update.effective_chat.id
        self.active_chats.add(chat_id)
        tracker = self.get_user_tracker(chat_id)
        pnl = tracker.total_realized_pnl()
        await update.message.reply_text(f"💰 Your Total Realized PnL: ${pnl:.2f}")


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
    params = {
        "user": target_wallet.lower(),
        "type": "TRADE",
        "start": last_timestamp,
        "sortBy": "TIMESTAMP",
        "sortDirection": "DESC"
    }

    session = get_http_session()
    retry_delay = INITIAL_RETRY_DELAY

    for attempt in range(MAX_RETRIES):
        try:
            logger.debug(f"Fetching trades (attempt {attempt + 1}/{MAX_RETRIES})")
            response = session.get(endpoint, params=params, timeout=10)

            # Handle rate limiting
            if response.status_code == 429:
                logger.warning(f"Rate limit hit. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY)
                continue

            response.raise_for_status()
            activities = response.json()

            # Parse trades
            trades = []
            for activity in activities:
                tx_hash = activity.get("transactionHash", "")

                # Skip duplicates
                if not tx_hash or tx_hash in seen_hashes:
                    continue

                try:
                    trade = Trade(
                        transaction_hash=tx_hash,
                        timestamp=activity.get("timestamp", 0),
                        market_id=activity.get("market", ""),
                        token_id=activity.get("asset", ""),
                        outcome=activity.get("outcome", ""),
                        side=activity.get("side", ""),
                        size=float(activity.get("size", 0)),
                        price=float(activity.get("price", 0)),
                        proxy_wallet=activity.get("user", "").lower()
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
            active_user_ids = list(notifier.user_trackers.keys())
        
        for chat_id in active_user_ids:
            user_tracker = notifier.get_user_tracker(chat_id)
            position_events = user_tracker.apply_trade(trade, my_size)
            # Send notifications to this specific user
            notifier.notify_events(position_events, chat_id=chat_id)
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
        position_tracker=position_tracker
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
