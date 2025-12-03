#!/usr/bin/env python3
"""
Polymarket Copy Trading Bot
===========================

⚠️  FINANCIAL RISK WARNING ⚠️
This script carries significant financial risk. Only run this bot with funds you can
afford to lose completely. Automated trading can result in rapid losses due to:
- Market volatility, API errors or delays, bugs in the code, network issues, liquidity problems

NEVER share your private key with anyone. Keep it secure at all times.

Target Wallets: 
- Primary: 0xeffcc79a8572940cee2238b44eac89f2c48fda88 (FirstOrder)
- Secondary: 0x5b6E4eF2952398983ccEE7E1EFA0fF0D3cf7B12a

The bot will copy trades from BOTH addresses and filter to only trade on:
- Solana Up or Down markets
- Bitcoin Up or Down markets  
- Ethereum Up or Down markets
- XRP Up or Down markets

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
Polymarket API Endpoints:
- REST CLOB: https://clob.polymarket.com/ (used for order placement)
- Data API: https://data-api.polymarket.com/ (used for trade monitoring)
- WebSocket CLOB: wss://ws-subscriptions-clob.polymarket.com/ws/ (available for real-time subscriptions)
- Real Time Data Socket: wss://ws-live-data.polymarket.com (available for real-time data streaming)

Documentation:
- Polymarket Data API: https://docs.polymarket.com/developers/misc-endpoints/data-api-activity
- Polymarket CLOB API: https://docs.polymarket.com/developers/CLOB/orders/create-order
- CLOB Get Trades: GET /data/trades (verify order execution, track trade statuses)
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

# Web3 for blockchain event queries (optional - only if available)
try:
    from web3 import Web3  # type: ignore
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False

# Polymarket CLOB client for order placement
try:
    from py_clob_client.client import ClobClient  # type: ignore
    from py_clob_client.clob_types import OrderArgs, OrderType  # type: ignore
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
    
    from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
    from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler, MessageHandler, filters
    from telegram.ext import JobQueue
except ImportError:
    print("ERROR: python-telegram-bot not installed. Run: pip install python-telegram-bot")
    sys.exit(1)

# Load environment variables
load_dotenv()

# ============================================================================
# CONFIGURATION
# ============================================================================

# Target wallets to copy trades from (can be multiple addresses)
# Primary target: 0xeffcc79a8572940cee2238b44eac89f2c48fda88 (FirstOrder)
# Secondary target: 0x5b6E4eF2952398983ccEE7E1EFA0fF0D3cf7B12a
TARGET_WALLETS = [
    "0xeffcc79a8572940cee2238b44eac89f2c48fda88",
    "0x5b6E4eF2952398983ccEE7E1EFA0fF0D3cf7B12a"
]
TARGET_WALLET = TARGET_WALLETS[0]  # Primary target for backward compatibility

# Allowed market IDs (conditionId) - only copy trades from these markets
# These correspond to the "Up or Down" markets for December 3, 6AM ET:
# - Solana Up or Down: https://polymarket.com/event/solana-up-or-down-december-3-6am-et
# - Bitcoin Up or Down: https://polymarket.com/event/bitcoin-up-or-down-december-3-6am-et
# - Ethereum Up or Down: https://polymarket.com/event/ethereum-up-or-down-december-3-6am-et
# - XRP Up or Down: https://polymarket.com/event/xrp-up-or-down-december-3-6am-et
#
# Market IDs can be configured via ALLOWED_MARKET_IDS env var (comma-separated)
# Or leave empty to auto-detect from market titles (filters by "Up or Down" pattern)
ALLOWED_MARKET_IDS = set()
if os.getenv("ALLOWED_MARKET_IDS"):
    # Parse comma-separated market IDs from environment
    market_ids_str = os.getenv("ALLOWED_MARKET_IDS", "")
    ALLOWED_MARKET_IDS = {mid.strip().lower() for mid in market_ids_str.split(",") if mid.strip()}
else:
    # Auto-detect: will filter by market title pattern "Up or Down" and date
    # This allows the bot to work without knowing exact market IDs upfront
    pass  # Logger not yet initialized, will log later

# Market title patterns to match (if market IDs not configured)
ALLOWED_MARKET_PATTERNS = [
    "solana up or down",
    "bitcoin up or down", 
    "ethereum up or down",
    "xrp up or down"
]

# Polymarket API Endpoints
# ========================
# REST CLOB: Used for all CLOB REST endpoints (order placement, market data, etc.)
# CLOB System: Hybrid-decentralized with off-chain matching and on-chain settlement
# - Orders are EIP712-signed structured data
# - Non-custodial: Settlement executed on-chain via signed order messages
# - Security: Exchange contract audited by Chainsecurity
# - Fees: Currently 0 bps for maker and taker (subject to change)
# - Trade Statuses: MATCHED -> MINED -> CONFIRMED (success) or RETRYING -> FAILED (failure)
# - Trade Verification: Use GET /data/trades endpoint to verify order execution
#   (filter by taker address, market, before/after timestamps)
CLOB_API_BASE_URL = "https://clob.polymarket.com"  # Central Limit Order Book (CLOB) REST API

# Data API: Delivers user data, holdings, and on-chain activities
DATA_API_BASE_URL = "https://data-api.polymarket.com"

# WebSocket CLOB: Used for real-time CLOB subscriptions (not currently used by this bot)
# CLOB_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/"

# Real Time Data Socket (RTDS): Real-time data streaming including crypto prices and comments
# RTDS_WS_URL = "wss://ws-live-data.polymarket.com"

# Polymarket Contract Addresses (Polygon Mainnet)
POLYMARKET_MAIN_CONTRACT = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045"  # Main Polymarket Contract
UMA_ADAPTER_CONTRACT = "0x65070BE91477460D8A7AeEb94ef92fe056C2f2A7"  # UMA Adapter Contract
POLYGON_RPC_URL = "https://polygon-rpc.com"  # Public Polygon RPC endpoint

# Read from environment variables with defaults
POLYMARKET_PRIVATE_KEY = os.getenv("POLYMARKET_PRIVATE_KEY", "")
POLYMARKET_PROXY_ADDRESS = os.getenv("POLYMARKET_PROXY_ADDRESS", "")  # Optional: Proxy address for Email/Magic or Browser Wallet login
SIGNATURE_TYPE = int(os.getenv("SIGNATURE_TYPE", "0"))  # 0=EOA, 1=Email/Magic, 2=Browser Wallet
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

# API Rate Limits (per Polymarket documentation)
# Data API: 200 requests / 10s (20 req/s) or 1200 requests / 1 minute
# Data API /trades: 75 requests / 10s (7.5 req/s)
# CLOB POST /order: 2400 requests / 10s burst, 24000 requests / 10 minutes sustained (40 req/s)
# With POLL_INTERVAL_SECONDS=2.0, we make ~0.5 requests/second, well within limits
# Rate limits use Cloudflare throttling - requests are queued/delayed, not rejected

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
    
    Terminology (per Polymarket glossary):
    - Token: A stake in a specific Yes/No outcome in a Market (also called Asset ID)
    - Market: A single event outcome with a pair of CLOB token IDs (Yes/No)
    - Event: A collection of related markets grouped under a common topic
    """
    transaction_hash: str
    timestamp: int
    market_id: str           # Market identifier (conditionId) - represents a single event outcome
    token_id: str            # Token ID (Asset ID) - represents stake in Yes/No outcome (0-1 price range)
    outcome: str             # Human-readable outcome (e.g., "Up" or "Down")
    side: str                # "BUY" or "SELL"
    size: float              # Number of shares (tokens)
    price: float             # Price per share (0-1 range, where 1 = $1 USDC at resolution)
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
    
    def get_trader_analytics_url(self, wallet_address: str) -> str:
        """Generate Polymarket Analytics URL for the trader's trades page."""
        if not wallet_address:
            return ""
        # Polymarket Analytics trader page
        return f"https://polymarketanalytics.com/traders/{wallet_address}#trades"
    
    def get_transaction_url(self) -> str:
        """Generate Polygon transaction URL for this trade."""
        if not self.transaction_hash:
            return ""
        # Polygon transaction link on Polygonscan
        return f"https://polygonscan.com/tx/{self.transaction_hash}"

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
    seen_resolution_ids: Optional[set[str]] = None
    
    def __post_init__(self) -> None:
        """Initialize seen_resolution_ids if None."""
        if self.seen_resolution_ids is None:
            self.seen_resolution_ids = set()

    def save(self, filepath: Path) -> None:
        """Save state to JSON file."""
        try:
            data = {
                "last_seen_timestamp": self.last_seen_timestamp,
                "seen_transaction_hashes": list(self.seen_transaction_hashes),
                "open_positions": self.open_positions,
                "closed_positions": self.closed_positions,
                "realized_pnl": self.realized_pnl,
                "seen_resolution_ids": list(self.seen_resolution_ids) if self.seen_resolution_ids else [],
            }
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2)
                logger.debug(f"State saved: {len(self.seen_transaction_hashes)} transactions")
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
                    seen_resolution_ids=set(data.get("seen_resolution_ids", []))
                )
                logger.info(f"Loaded state: {len(state.seen_transaction_hashes)} transactions")
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
        trade_history: Optional[List[Dict]] = None,
        resolved_positions: Optional[List[Dict]] = None,
        daily_hourly_pnl: Optional[Dict[str, Dict]] = None  # {date: {hour: pnl}}
    ) -> None:
        self.open_positions: Dict[str, Dict] = open_positions or {}
        self.closed_positions: List[Dict] = closed_positions or []
        self.realized_pnl: float = realized_pnl
        self.trade_history: List[Dict] = trade_history or []  # Track all trades for volume calculation
        self.resolved_positions: List[Dict] = resolved_positions or []  # Track resolved market positions
        self.daily_hourly_pnl: Dict[str, Dict] = daily_hourly_pnl or {}  # {date: {hour: pnl}}
        self._lock = threading.RLock()
    
    def save_to_file(self, filepath: Path) -> None:
        """Save tracker state to JSON file."""
        try:
            with self._lock:
                # Recalculate PnL from closed positions before saving to ensure accuracy
                trade_pnl = sum(float(entry.get("realized_pnl", 0.0)) for entry in self.closed_positions)
                resolution_pnl = sum(float(entry.get("realized_pnl", 0.0)) for entry in self.resolved_positions)
                total_pnl = trade_pnl + resolution_pnl
                
                if abs(total_pnl - self.realized_pnl) > 0.01:
                    logger.warning(f"⚠️  PnL discrepancy detected before save: stored={self.realized_pnl:.2f}, "
                                 f"recalculated={total_pnl:.2f} (trade_pnl={trade_pnl:.2f}, resolution_pnl={resolution_pnl:.2f})")
                    logger.info(f"Correcting PnL before save: {self.realized_pnl:.2f} -> {total_pnl:.2f}")
                    self.realized_pnl = total_pnl
                
                # Validate position consistency
                total_open_value = sum(
                    abs(float(pos.get("net_size", 0.0))) * float(pos.get("avg_entry_price", 0.0))
                    for pos in self.open_positions.values()
                )
                logger.debug(f"Position validation: {len(self.open_positions)} open positions, "
                           f"total_value=${total_open_value:.2f}, {len(self.closed_positions)} closed, "
                           f"{len(self.trade_history)} trades in history")
                
                # Clean up old daily hourly PnL data (keep only last 7 days)
                today = datetime.now().date()
                cleaned_daily_pnl = {}
                for date_str, hourly_data in self.daily_hourly_pnl.items():
                    try:
                        date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
                        if (today - date_obj).days <= 7:  # Keep last 7 days
                            cleaned_daily_pnl[date_str] = hourly_data
                    except:
                        pass
                
                data = {
                    "open_positions": self.open_positions,
                    "closed_positions": self.closed_positions,
                    "realized_pnl": self.realized_pnl,
                    "trade_history": self.trade_history,
                    "resolved_positions": self.resolved_positions,
                    "daily_hourly_pnl": cleaned_daily_pnl
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
                    trade_history=data.get("trade_history", []),
                    resolved_positions=data.get("resolved_positions", [])
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
                    
                    # When hedging: we're BUYING the opposite side, so we create a position
                    # The new position size is the full signed_size (what we bought)
                    # This is because hedging means buying both sides, creating a new position
                    new_position_size = signed_size  # Full amount bought becomes the hedge position
                    
                    # Record closed position
                    # Get opened_at from the opposite position (when it was first opened)
                    opened_at = opposite_pos.get("opened_at", opposite_pos.get("last_update", timestamp))
                    closed_entry = {
                        "token_id": opposite_pos.get("token_id", ""),
                        "market_id": trade.market_id,
                        "outcome": f"{opposite_outcome} → {trade.outcome}",
                        "size": closing_size,
                        "entry_price": entry_price,
                        "exit_price": exit_price,
                        "entry_size": closing_size,  # Size when opened
                        "realized_pnl": pnl,
                        "opened_at": opened_at,
                        "closed_at": timestamp,
                        "type": "HEDGE_CLOSE" if abs(remaining_opposite) < POSITION_EPSILON else "PARTIAL_HEDGE",
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
                    
                    # Create new hedge position (the opposite side we just bought)
                    # This is the hedge position that offsets the closed portion
                    if abs(new_position_size) >= POSITION_EPSILON:
                        # Check if we already have a position for this token_id
                        existing_new = self.open_positions.get(trade.token_id)
                        if existing_new:
                            # Add to existing position
                            existing_size = existing_new.get("net_size", 0.0)
                            total_size = existing_size + new_position_size
                            if abs(total_size) < POSITION_EPSILON:
                                self.open_positions.pop(trade.token_id, None)
                            else:
                                # Recalculate average price
                                total_abs = abs(existing_size) + abs(new_position_size)
                                new_avg = (
                                    abs(existing_size) * existing_new.get("avg_entry_price", 0.0) +
                                    abs(new_position_size) * trade.price
                                ) / total_abs
                                existing_new["net_size"] = total_size
                                existing_new["avg_entry_price"] = new_avg
                                existing_new["last_update"] = timestamp
                        else:
                            # Create new hedge position (the opposite side we just bought)
                            new_position = {
                                "token_id": trade.token_id,
                                "market_id": trade.market_id,
                                "outcome": trade.outcome or "Unknown",
                                "net_size": new_position_size,
                                "avg_entry_price": trade.price,
                                "opened_at": timestamp,  # Track when position was first opened
                                "last_update": timestamp,
                                "title": trade.title or "",
                                "coin_name": trade.get_coin_name()
                            }
                            self.open_positions[trade.token_id] = new_position
                    
                    # Make hedging message very clear
                    pnl_emoji = "✅" if pnl >= 0 else "❌"
                    pnl_sign = "+" if pnl >= 0 else ""
                    
                    coin_name = trade.get_coin_name() or opposite_pos.get("coin_name", "Unknown")
                    outcome_emoji = "🟢" if trade.outcome.upper() == "UP" else "🔴"
                    opposite_emoji = "🔴" if trade.outcome.upper() == "UP" else "🟢"
                    
                    # Convert prices to cents for display
                    entry_price_cents = int(entry_price * 100)
                    exit_price_cents = int(exit_price * 100)
                    
                    if closed_entry["type"] == "HEDGE_CLOSE":
                        message = (
                            f"🔄 HEDGE {coin_name}\n"
                            f"{trade.title or 'Unknown'}\n"
                            f"{opposite_emoji}-{closing_size:.1f} {opposite_outcome} @ {entry_price_cents}¢ | {outcome_emoji}+{closing_size:.1f} {trade.outcome} @ {exit_price_cents}¢\n"
                            f"{pnl_emoji} PnL: {pnl_sign}${pnl:.2f} ✅ Closed"
                        )
                    else:  # PARTIAL_HEDGE
                        message = (
                            f"🔄 PARTIAL HEDGE {coin_name}\n"
                            f"{trade.title or 'Unknown'}\n"
                            f"{opposite_emoji}-{closing_size:.1f} {opposite_outcome} @ {entry_price_cents}¢ | {outcome_emoji}+{closing_size:.1f} {trade.outcome} @ {exit_price_cents}¢\n"
                            f"{pnl_emoji} PnL: {pnl_sign}${pnl:.2f} ⚠️ Open"
                        )
                    
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
                    "opened_at": timestamp,  # Track when position was first opened
                    "last_update": timestamp,
                    "title": trade.title or "",
                    "coin_name": trade.get_coin_name()
                }
                self.open_positions[trade.token_id] = new_position
                direction = "long" if signed_size > 0 else "short"
                coin_name = trade.get_coin_name()
                outcome_emoji = "🟢" if trade.outcome.upper() == "UP" else "🔴"
                direction_emoji = "📈" if direction == "long" else "📉"
                
                events.append(PositionEvent(
                    event_type="OPENED",
                    message=(
                        f"🟢 OPEN {outcome_emoji} {coin_name} {trade.outcome}\n"
                        f"{trade.title or 'Unknown'}\n"
                        f"{direction_emoji} {abs(signed_size):.1f} @ ${trade.price:.4f} (${abs(signed_size) * trade.price:.1f})"
                    ),
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
                coin_name = trade.get_coin_name()
                outcome_emoji = "🟢" if trade.outcome.upper() == "UP" else "🔴"
                direction_emoji = "📈" if direction == "long" else "📉"
                
                events.append(PositionEvent(
                    event_type="INCREASED",
                    message=(
                        f"📈 +{abs(signed_size):.1f} {outcome_emoji} {coin_name} {trade.outcome}\n"
                        f"{trade.title or 'Unknown'}\n"
                        f"Added: ${abs(signed_size) * trade.price:.1f} | Total: {abs(existing['net_size']):.1f} @ ${existing['avg_entry_price']:.4f}"
                    ),
                    payload=existing.copy()
                ))
                return events

            # Opposite direction -> closing/reversing
            closing_size = min(abs(current_size), abs(signed_size))
            pnl = closing_size * (trade.price - existing["avg_entry_price"]) * (
                1 if current_size > 0 else -1
            )
            self.realized_pnl += pnl
            
            # Get opened_at for closed position record
            opened_at = existing.get("opened_at", existing.get("last_update", timestamp))

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
                "entry_size": closing_size,
                "realized_pnl": pnl,
                "opened_at": opened_at,
                "closed_at": timestamp,
                "type": close_type,
                "title": trade.title or existing.get("title", ""),
                "coin_name": trade.get_coin_name() or existing.get("coin_name", "Unknown")
            }
            self.closed_positions.append(closed_entry)
            if len(self.closed_positions) > MAX_CLOSED_POSITIONS:
                self.closed_positions = self.closed_positions[-MAX_CLOSED_POSITIONS:]

            coin_name = trade.get_coin_name() or existing.get("coin_name", "Unknown")
            outcome_emoji = "🟢" if trade.outcome.upper() == "UP" else "🔴"
            pnl_emoji = "✅" if pnl >= 0 else "❌"
            pnl_sign = "+" if pnl >= 0 else ""

            if abs(remaining) < POSITION_EPSILON:
                # Fully closed
                message = (
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🔴 POSITION FULLY CLOSED\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"📊 MARKET:\n"
                    f"   {outcome_emoji} {coin_name} {trade.outcome}\n"
                    f"   {trade.title or existing.get('title', 'Unknown Market')}\n\n"
                    f"💼 CLOSED POSITION:\n"
                    f"   📉 Size: {closing_size:.2f} shares\n"
                    f"   💵 Exit Price: ${trade.price:.4f}\n"
                    f"   💰 Entry Price: ${existing.get('avg_entry_price', 0.0):.4f}\n"
                    f"   💲 Value Closed: ${closing_size * trade.price:.2f} USDC\n\n"
                    f"💰 RESULT:\n"
                    f"   {pnl_emoji} Realized PnL: {pnl_sign}${pnl:.2f}"
                )
                self.open_positions.pop(trade.token_id, None)
            elif (remaining > 0 and current_size < 0) or (remaining < 0 and current_size > 0):
                # Position flipped to opposite direction
                new_position = {
                    "token_id": trade.token_id,
                    "market_id": trade.market_id,
                    "outcome": trade.outcome or "Unknown",
                    "net_size": remaining,
                    "avg_entry_price": trade.price,
                    "opened_at": timestamp,  # New position opened at reversal
                    "last_update": timestamp,
                    "title": trade.title or existing.get("title", ""),
                    "coin_name": coin_name
                }
                self.open_positions[trade.token_id] = new_position
                new_direction = "long" if remaining > 0 else "short"
                direction_emoji = "📈" if new_direction == "long" else "📉"
                message = (
                    f"🔄 REVERSE {outcome_emoji} {coin_name} {trade.outcome}\n"
                    f"{trade.title or existing.get('title', 'Unknown')}\n"
                    f"Close: {closing_size:.1f} @ ${trade.price:.4f} | {pnl_emoji} PnL: {pnl_sign}${pnl:.2f}\n"
                    f"New: {direction_emoji} {abs(remaining):.1f} @ ${trade.price:.4f}"
                )
            else:
                # Partially closed
                existing["net_size"] = remaining
                existing["last_update"] = timestamp
                remaining_direction = "long" if remaining > 0 else "short"
                direction_emoji = "📈" if remaining_direction == "long" else "📉"
                message = (
                    f"🔴 PARTIAL CLOSE {outcome_emoji} {coin_name} {trade.outcome}\n"
                    f"{trade.title or existing.get('title', 'Unknown')}\n"
                    f"Closed: {closing_size:.1f} @ ${trade.price:.4f} | {pnl_emoji} PnL: {pnl_sign}${pnl:.2f}\n"
                    f"Remaining: {direction_emoji} {abs(remaining):.1f} @ ${existing.get('avg_entry_price', 0.0):.4f}"
                )

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
        """Get total realized PnL. Includes both trade PnL and resolution payouts."""
        with self._lock:
            # Calculate PnL from closed positions (trades)
            trade_pnl = sum(float(entry.get("realized_pnl", 0.0)) for entry in self.closed_positions)
            
            # Calculate PnL from resolved positions (market payouts)
            # When a market resolves: winning positions pay $1/share, losing pay $0/share
            resolution_pnl = sum(float(entry.get("payout", 0.0)) - float(entry.get("cost_basis", 0.0)) 
                                for entry in self.resolved_positions)
            
            total_pnl = trade_pnl + resolution_pnl
            
            # Use the stored value, but log if there's a discrepancy
            if abs(total_pnl - self.realized_pnl) > 0.01:
                logger.warning(f"PnL discrepancy detected: stored={self.realized_pnl:.2f}, recalculated={total_pnl:.2f} "
                             f"(trade_pnl={trade_pnl:.2f}, resolution_pnl={resolution_pnl:.2f}). Using recalculated value.")
                self.realized_pnl = total_pnl
            return self.realized_pnl
    
    def calculate_unrealized_pnl(self, current_prices: Dict[str, float]) -> Dict:
        """
        Calculate unrealized PnL for all open positions based on current market prices.
        
        Args:
            current_prices: Dict mapping token_id -> current_price (0-1 range)
        
        Returns:
            Dict with:
            - total_unrealized_pnl: Total unrealized PnL
            - positions: List of position details with unrealized PnL
        """
        with self._lock:
            total_unrealized = 0.0
            positions_detail = []
            
            for token_id, pos in self.open_positions.items():
                size = pos.get("net_size", 0.0)
                entry_price = pos.get("avg_entry_price", 0.0)
                outcome = pos.get("outcome", "Unknown")
                coin_name = pos.get("coin_name", "Unknown")
                title = pos.get("title", "Unknown Market")
                
                if abs(size) < POSITION_EPSILON or entry_price <= 0:
                    continue
                
                # Get current price for this token
                current_price = current_prices.get(token_id, None)
                if current_price is None:
                    # Price not available - skip this position
                    continue
                
                # Calculate unrealized PnL
                # For long positions (size > 0): PnL = (current_price - entry_price) * size
                # For short positions (size < 0): PnL = (entry_price - current_price) * abs(size)
                if size > 0:
                    # Long position
                    unrealized_pnl = (current_price - entry_price) * size
                else:
                    # Short position
                    unrealized_pnl = (entry_price - current_price) * abs(size)
                
                total_unrealized += unrealized_pnl
                
                positions_detail.append({
                    "token_id": token_id,
                    "coin_name": coin_name,
                    "outcome": outcome,
                    "title": title,
                    "size": size,
                    "entry_price": entry_price,
                    "current_price": current_price,
                    "unrealized_pnl": unrealized_pnl
                })
            
            return {
                "total_unrealized_pnl": total_unrealized,
                "positions": positions_detail
            }
    
    def apply_resolution(self, market_id: str, winning_outcome: str, resolution_timestamp: int, resolved_price: Optional[float] = None) -> float:
        """
        Apply market resolution to open positions.
        When a market resolves:
        - Winning positions: payout = size * (resolved_price / entry_price)
        - Losing positions: payout = $0 (full loss)
        
        Args:
            market_id: The conditionId of the resolved market
            winning_outcome: "Up" or "Down" - the winning outcome
            resolution_timestamp: When the market resolved
            resolved_price: The final resolved price (0-1 range). If None, defaults to 1.0 (100¢)
            
        Returns:
            Total PnL from this resolution
        """
        with self._lock:
            total_resolution_pnl = 0.0
            
            # Default resolved price to 1.0 (100¢) if not provided (backward compatibility)
            if resolved_price is None:
                resolved_price = 1.0
            
            # Find all open positions in this market
            positions_to_resolve = []
            for token_id, pos in list(self.open_positions.items()):
                if pos.get("market_id") == market_id:
                    positions_to_resolve.append((token_id, pos))
            
            for token_id, pos in positions_to_resolve:
                outcome = pos.get("outcome", "")
                size = abs(float(pos.get("net_size", 0.0)))
                entry_price = float(pos.get("avg_entry_price", 0.0))
                cost_basis = size * entry_price
                
                # Determine if this position won
                is_winner = (outcome.upper() == winning_outcome.upper())
                
                if is_winner:
                    # Winning position: payout based on resolved_price/entry_price ratio
                    # Example: invested $1 at 65¢, resolved at 99¢
                    # payout = $1 * (0.99 / 0.65) = $1.52
                    if entry_price > 0:
                        payout = size * (resolved_price / entry_price)
                    else:
                        payout = size * resolved_price  # Fallback if entry_price is 0
                    pnl = payout - cost_basis
                else:
                    # Losing position: pays $0 (full loss)
                    payout = 0.0
                    pnl = -cost_basis  # Lost the entire cost basis
                
                total_resolution_pnl += pnl
                
                # Record the resolution
                resolution_entry = {
                    "market_id": market_id,
                    "token_id": token_id,
                    "outcome": outcome,
                    "winning_outcome": winning_outcome,
                    "size": size,
                    "entry_price": entry_price,
                    "resolved_price": resolved_price,
                    "cost_basis": cost_basis,
                    "payout": payout,
                    "realized_pnl": pnl,
                    "resolved_at": resolution_timestamp,
                    "title": pos.get("title", ""),
                    "coin_name": pos.get("coin_name", "Unknown")
                }
                self.resolved_positions.append(resolution_entry)
                
                # Remove from open positions
                self.open_positions.pop(token_id, None)
                
                logger.info(f"Market resolved: {outcome} {'WON' if is_winner else 'LOST'} - "
                          f"Size: {size:.2f}, Entry: ${entry_price:.4f}, Resolved: ${resolved_price:.4f}, "
                          f"Cost: ${cost_basis:.2f}, Payout: ${payout:.2f}, PnL: ${pnl:.2f}")
            
            # Update total PnL
            self.realized_pnl += total_resolution_pnl
            
            return total_resolution_pnl
    
    def record_hourly_pnl(self, hour_start: datetime, pnl: float) -> None:
        """Record hourly PnL for the current day."""
        with self._lock:
            date_str = hour_start.strftime("%Y-%m-%d")
            hour_key = hour_start.strftime("%I:%M %p")  # e.g., "12:00 PM"
            
            if date_str not in self.daily_hourly_pnl:
                self.daily_hourly_pnl[date_str] = {}
            
            # Accumulate PnL for this hour (in case called multiple times)
            if hour_key in self.daily_hourly_pnl[date_str]:
                self.daily_hourly_pnl[date_str][hour_key] += pnl
            else:
                self.daily_hourly_pnl[date_str][hour_key] = pnl
    
    def get_today_hourly_pnl(self) -> Dict[str, float]:
        """Get hourly PnL breakdown for today."""
        with self._lock:
            today_str = datetime.now().strftime("%Y-%m-%d")
            return self.daily_hourly_pnl.get(today_str, {}).copy()
    
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
            
            # Get PnL for the period (from trades only)
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
            # PnL from closed trades only
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
        
        # Trading state: True = LIVE trading, False = STOPPED (draft mode)
        self.trading_state_file = Path("trading_state.json")
        self.is_live_trading = self._load_trading_state()
        
        # Initialize Telegram bot if enabled
        if self.enabled:
            self._init_telegram()
    
    def _load_trading_state(self) -> bool:
        """Load trading state from file. Defaults to False (stopped/draft mode) for safety."""
        if self.trading_state_file.exists():
            try:
                with open(self.trading_state_file, 'r') as f:
                    data = json.load(f)
                    return bool(data.get("is_live_trading", False))
            except Exception as e:
                logger.warning(f"Failed to load trading state: {e}. Defaulting to STOPPED.")
        return False  # Default to stopped for safety
    
    def _save_trading_state(self) -> None:
        """Save trading state to file."""
        try:
            with open(self.trading_state_file, 'w') as f:
                json.dump({"is_live_trading": self.is_live_trading}, f, indent=2)
            logger.info(f"💾 Trading state saved: {'🟢 LIVE' if self.is_live_trading else '🔴 STOPPED'}")
        except Exception as e:
            logger.error(f"Failed to save trading state: {e}")

    def _init_telegram(self) -> None:
        """Initialize Telegram bot handlers and start polling."""
        if not self.enabled:
            logger.info("Telegram integration disabled (set TELEGRAM_BOT_TOKEN to enable).")
            return

        # Load trading state
        logger.info(f"📊 Trading Status: {'🟢 LIVE' if self.is_live_trading else '🔴 STOPPED (Draft Mode)'}")
        
        try:
            # Build Application (timezone is already patched at module level)
            self.application = Application.builder().token(self.bot_token).build()
            
            # Add handlers
            self.application.add_handler(CommandHandler("start", self._handle_start))
            self.application.add_handler(CommandHandler("menu", self._handle_menu))
            self.application.add_handler(CommandHandler("help", self._handle_help))
            self.application.add_handler(CommandHandler("pastres", self._handle_past_resolutions))
            self.application.add_handler(CommandHandler("status", self._handle_status))
            self.application.add_handler(CommandHandler("status", self._handle_status))
            self.application.add_handler(CommandHandler("openpositions", self._handle_open_positions))
            self.application.add_handler(CommandHandler("closedpositions", self._handle_closed_positions))
            self.application.add_handler(CommandHandler("pnl", self._handle_pnl))
            self.application.add_handler(CommandHandler("pnltoday", self._handle_pnl_today))
            self.application.add_handler(CommandHandler("reset", self._handle_reset))
            self.application.add_handler(CommandHandler("money", self._handle_money))
            self.application.add_handler(CommandHandler("backfill", self._handle_backfill))
            self.application.add_handler(CommandHandler("backfillall", self._handle_backfill_all))
            self.application.add_handler(CommandHandler("live", self._handle_live))
            self.application.add_handler(CommandHandler("stop", self._handle_stop))
            self.application.add_handler(CommandHandler("prices", self._handle_prices))
            # Add callback query handler for inline keyboard buttons
            self.application.add_handler(CallbackQueryHandler(self._handle_callback))
            # Add message handler for reply keyboard buttons
            self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self._handle_keyboard_message))
            logger.info("✅ Registered all command handlers including menu system")
            
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
                
                # Schedule end-of-hour unrealized PnL calculation (runs at :59:59 - last second)
                # Calculate seconds until next :59:59
                next_end_of_hour = now.replace(minute=59, second=59, microsecond=0)
                if next_end_of_hour <= now:
                    # Already past :59:59 this hour, schedule for next hour
                    next_end_of_hour = next_end_of_hour + timedelta(hours=1)
                seconds_until_end_of_hour = (next_end_of_hour - now).total_seconds()
                
                job_queue.run_repeating(
                    self._calculate_end_of_hour_pnl,
                    interval=3600,  # Every hour
                    first=seconds_until_end_of_hour,
                    name="end_of_hour_pnl"
                )
                logger.info(f"Scheduled end-of-hour PnL calculation at :59:59. First calculation in {int(seconds_until_end_of_hour)}s")
            
            # Start polling in background thread
            def start_polling():
                try:
                    if self.application is not None:
                        self.application.run_polling(
                            allowed_updates=Update.ALL_TYPES,
                            drop_pending_updates=True,
                            stop_signals=None,  # Don't handle signals in thread
                            close_loop=False  # Don't close event loop when stopping
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

    async def send_message_async(self, text: str, chat_id: int, parse_mode: Optional[str] = None) -> None:
        """Send message to a specific chat (async)."""
        if not self.enabled or not self.application:
            return
        try:
            kwargs = {"chat_id": chat_id, "text": text}
            if parse_mode:
                kwargs["parse_mode"] = parse_mode
            await self.application.bot.send_message(**kwargs)
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

    def send_message_to_all(self, text: str, parse_mode: Optional[str] = None) -> None:
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
                loop.run_until_complete(self.send_message_async(text, chat_id, parse_mode=parse_mode))
            except Exception as exc:
                logger.warning(f"Failed to send message to chat {chat_id}: {exc}")
                # Remove chat if it's no longer valid (user blocked bot, etc.)
                self.active_chats.discard(chat_id)

    async def _handle_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /start command - load or create user state and show menu."""
        if not update.effective_chat:
            return
        chat_id = update.effective_chat.id
        self.active_chats.add(chat_id)
        
        # Load existing tracker or create new one
        tracker = self.get_user_tracker(chat_id)
        
        # Get target wallet from instance variable
        target_wallet_short = self.target_wallet[:10] + "..." + self.target_wallet[-8:] if len(self.target_wallet) > 18 else self.target_wallet
        
        # Get current stats
        total_pnl = tracker.total_realized_pnl()
        open_count = len(tracker.snapshot_open_positions())
        closed_count = len(tracker.snapshot_closed_positions(limit=0))
        
        welcome_text = (
            "🤖 *Polymarket Copy Trading Bot*\n\n"
            f"📊 *Target Wallet:* `{target_wallet_short}`\n\n"
            f"📈 *Your Stats:*\n"
            f"   💰 Total PnL: ${total_pnl:+.2f}\n"
            f"   📊 Open Positions: {open_count}\n"
            f"   📉 Closed Trades: {closed_count}\n\n"
            "✨ *New Features:*\n"
            "   ✅ Enhanced CLOB integration\n"
            "   ✅ Rate limit handling\n"
            "   ✅ Trade status tracking\n"
            "   ✅ 24-hour backfill\n"
            "   ✅ Market resolution detection\n"
            "   ✅ Comprehensive PnL analytics\n\n"
            "🚀 *Bot is active and monitoring trades!*\n"
            "Use /menu or the keyboard below to explore."
        )
        
        # Show both inline and reply keyboards
        inline_keyboard = self._create_menu_keyboard()
        reply_keyboard = self._create_reply_keyboard()
        if update.message:
            await update.message.reply_text(
                welcome_text, 
                parse_mode='Markdown',
                reply_markup=inline_keyboard
            )
            # Send a separate message with the persistent keyboard
            await update.message.reply_text(
                "👇 *Quick Access Keyboard:*\n\n"
                "💡 *Tip:* Use buttons below or commands like /menu, /help",
                parse_mode='Markdown',
                reply_markup=reply_keyboard
            )
        logger.info(f"User started: chat_id={chat_id}, pnl=${total_pnl:.2f}")
    
    def _create_menu_keyboard(self) -> InlineKeyboardMarkup:
        """Create the main menu inline keyboard."""
        keyboard = [
            [
                InlineKeyboardButton("📊 PnL - Hour", callback_data="pnl_hour"),
                InlineKeyboardButton("📅 PnL - Today", callback_data="pnl_today")
            ],
            [
                InlineKeyboardButton("📆 PnL - Week", callback_data="pnl_week"),
                InlineKeyboardButton("📆 PnL - Month", callback_data="pnl_month")
            ],
            [
                InlineKeyboardButton("💰 PnL - All Time", callback_data="pnl_alltime"),
                InlineKeyboardButton("🔄 Backfill", callback_data="backfill")
            ],
            [
                InlineKeyboardButton("📈 Open Positions", callback_data="open_positions"),
                InlineKeyboardButton("📉 Closed Trades", callback_data="closed_trades")
            ],
            [
                InlineKeyboardButton("💵 Money Stats", callback_data="money_stats"),
                InlineKeyboardButton("💹 Live Prices", callback_data="live_prices")
            ],
            [
                InlineKeyboardButton("🔄 Reset", callback_data="reset")
            ],
            [
                InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")
            ]
        ]
        return InlineKeyboardMarkup(keyboard)
    
    def _create_reply_keyboard(self) -> ReplyKeyboardMarkup:
        """Create the persistent reply keyboard menu."""
        keyboard = [
            [
                KeyboardButton("📊 PnL Hour"),
                KeyboardButton("📅 PnL Today")
            ],
            [
                KeyboardButton("📆 PnL Week"),
                KeyboardButton("📆 PnL Month")
            ],
            [
                KeyboardButton("💰 PnL All Time"),
                KeyboardButton("🔄 Backfill")
            ],
            [
                KeyboardButton("🔄 Backfill All (24h)")
            ],
            [
                KeyboardButton("📈 Open Positions"),
                KeyboardButton("📉 Closed Trades")
            ],
            [
                KeyboardButton("💵 Money Stats"),
                KeyboardButton("🏠 Menu")
            ]
        ]
        return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    
    async def _handle_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /menu command - show main menu."""
        if not update.effective_chat:
            return
        chat_id = update.effective_chat.id
        self.active_chats.add(chat_id)
        
        tracker = self.get_user_tracker(chat_id)
        total_pnl = tracker.total_realized_pnl()
        open_count = len(tracker.snapshot_open_positions())
        closed_count = len(tracker.snapshot_closed_positions(limit=0))
        resolved_count = len(tracker.resolved_positions)
        
        menu_text = (
            "📊 *Main Menu*\n\n"
            f"💰 *Total PnL:* ${total_pnl:+.2f}\n"
            f"📈 *Open Positions:* {open_count}\n"
            f"📉 *Closed Trades:* {closed_count}\n"
            f"🎯 *Resolved Markets:* {resolved_count}\n\n"
            "✨ *Features:*\n"
            "   📊 Real-time PnL tracking\n"
            "   🔄 24-hour backfill\n"
            "   📈 Position analytics\n"
            "   💵 Money stats\n"
            "   🎯 Market resolutions\n\n"
            "Select an option below:"
        )
        
        inline_keyboard = self._create_menu_keyboard()
        reply_keyboard = self._create_reply_keyboard()
        if update.message:
            await update.message.reply_text(
                menu_text,
                parse_mode='Markdown',
                reply_markup=reply_keyboard
            )

    async def _handle_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /help command."""
        if not update.effective_chat:
            return
        chat_id = update.effective_chat.id
        self.active_chats.add(chat_id)  # Register on any command
        help_text = (
            "🤖 *Polymarket Copy Bot - Commands*\n\n"
            "📊 *Status & Analytics:*\n"
            "/status - Quick health summary\n"
            "/menu - Main menu with all options\n"
            "/pnl - Current hour PnL\n"
            "/pnltoday - Today's PnL (since 12 AM)\n\n"
            "📈 *Positions & Trades:*\n"
            "/openpositions - List open positions\n"
            "/closedpositions - Recent closed trades\n"
            "/pastres - Past resolutions for open trades\n"
            "/money - Volume & trade statistics\n\n"
            "🔄 *Data Management:*\n"
            "/backfill - Reset & refresh current hour\n"
            "/backfillall - Reset & refresh last 24 hours\n"
            "/reset - Clear all positions & trades\n\n"
            "✨ *New Features:*\n"
            "   • Enhanced CLOB order placement\n"
            "   • Rate limit protection\n"
            "   • Trade status tracking\n"
            "   • Market resolution detection\n"
            "   • Comprehensive analytics\n\n"
            "💡 Use the keyboard buttons for quick access!"
        )
        reply_keyboard = self._create_reply_keyboard()
        if update.message:
            await update.message.reply_text(
                help_text,
                parse_mode='Markdown',
                reply_markup=reply_keyboard
            )

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
        closed_count = len(tracker.snapshot_closed_positions(limit=0))
        resolved_count = len(tracker.resolved_positions)
        
        status_lines = [
            "📊 *Bot Status*\n",
            f"💰 *Total PnL:* ${pnl:+.2f}",
            f"📈 *Open Positions:* {len(open_positions)}",
            f"📉 *Closed Trades:* {closed_count}",
            f"🎯 *Resolved Markets:* {resolved_count}",
            "",
            "✨ *Bot Features:*",
            "   ✅ Real-time trade copying",
            "   ✅ Rate limit protection",
            "   ✅ Market resolution tracking",
            "   ✅ Comprehensive analytics"
        ]
        if last_closed:
            lc = last_closed[-1]
            status_lines.append("")
            status_lines.append("📊 *Last Closed Trade:*")
            status_lines.append(
                f"   {lc.get('outcome')} {lc.get('type')} - "
                f"PnL ${lc.get('realized_pnl', 0.0):+.2f}"
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
        reply_keyboard = self._create_reply_keyboard()
        if update.message:
            await update.message.reply_text(
                "\n".join(lines),
                reply_markup=reply_keyboard
            )

    async def _handle_past_resolutions(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /pastres command - show previous resolutions for markets with open trades."""
        if not update.effective_chat:
            return
        chat_id = update.effective_chat.id
        self.active_chats.add(chat_id)
        tracker = self.get_user_tracker(chat_id)
        
        open_positions = tracker.snapshot_open_positions()
        if not open_positions:
            reply_keyboard = self._create_reply_keyboard()
            if update.message:
                await update.message.reply_text(
                    "📭 No open positions.\n\n"
                    "💡 Open a position first to see its past resolution.",
                    reply_markup=reply_keyboard
                )
            return
        
        # Get all resolved positions
        resolved_positions = tracker.resolved_positions
        
        # Group open positions by coin name
        coin_positions = {}  # coin_name -> list of positions
        for pos in open_positions:
            coin = pos.get("coin_name", "Unknown")
            if coin not in coin_positions:
                coin_positions[coin] = []
            coin_positions[coin].append(pos)
        
        # Find most recent resolution for each coin
        lines = ["🎯 *Past Resolutions for Open Trades*\n"]
        found_any = False
        
        for coin, positions in coin_positions.items():
            # Find all resolutions for this coin
            coin_resolutions = [
                rp for rp in resolved_positions
                if rp.get("coin_name", "").upper() == coin.upper()
            ]
            
            if coin_resolutions:
                # Sort by resolved_at timestamp (most recent first)
                coin_resolutions.sort(key=lambda x: x.get("resolved_at", 0), reverse=True)
                most_recent = coin_resolutions[0]
                
                resolved_at = most_recent.get("resolved_at", 0)
                resolved_time = datetime.fromtimestamp(resolved_at).strftime("%B %d, %Y %I:%M %p") if resolved_at else "Unknown"
                winning_outcome = most_recent.get("winning_outcome", "Unknown")
                title = most_recent.get("title", "")
                
                # Count open positions for this coin
                open_count = len(positions)
                total_size = sum(abs(float(p.get("net_size", 0.0))) for p in positions)
                
                outcome_emoji = "🟢" if winning_outcome.upper() == "UP" else "🔴"
                
                lines.append(f"{outcome_emoji} *{coin}*\n")
                if title:
                    lines.append(f"   📋 {title}\n")
                lines.append(f"   🎯 Resolved: *{winning_outcome}*\n")
                lines.append(f"   ⏰ Time: {resolved_time}\n")
                lines.append(f"   📊 Your Open: {open_count} position(s), {total_size:.1f} shares\n")
                lines.append("")
                found_any = True
            else:
                # No resolution found for this coin
                open_count = len(positions)
                lines.append(f"❓ *{coin}*\n")
                lines.append(f"   📊 Your Open: {open_count} position(s)\n")
                lines.append(f"   ⚠️ No resolution data found\n")
                lines.append("")
        
        if not found_any:
            lines.append("⚠️ *No resolution data available*\n\n")
            lines.append("💡 Resolutions are tracked when markets resolve.\n")
            lines.append("   Run /backfillall to process historical resolutions.")
        else:
            lines.append("💡 *Note:* Shows most recent resolution per coin.\n")
            lines.append("   Use /backfillall to refresh resolution data.")
        
        reply_keyboard = self._create_reply_keyboard()
        if update.message:
            await update.message.reply_text(
                "\n".join(lines),
                parse_mode='Markdown',
                reply_markup=reply_keyboard
            )

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
        
        # Calculate current hour (same as /backfill)
        # If it's 2:23 PM, we want 2:00 PM - 3:00 PM (the current hour)
        now = datetime.now()
        hour_start = now.replace(minute=0, second=0, microsecond=0)
        hour_end = hour_start + timedelta(hours=1)  # End of current hour
        start_timestamp = int(hour_start.timestamp())
        end_timestamp = int(hour_end.timestamp())
        
        # Get hourly stats (same method used by /backfill)
        stats = tracker.get_hourly_stats(start_timestamp, end_timestamp)
        hourly_pnl = stats['pnl']
        peak_concurrent_exposure = stats.get('peak_concurrent_exposure', 0.0)
        
        # Record this hour's PnL for today's tracking
        tracker.record_hourly_pnl(hour_start, hourly_pnl)
        
        # Get today's hourly PnL breakdown
        today_hourly_pnl = tracker.get_today_hourly_pnl()
        
        open_positions = tracker.snapshot_open_positions()
        closed_count = len(tracker.snapshot_closed_positions(limit=0))
        
        # Calculate current/live concurrent exposure from open positions
        live_concurrent_exposure = sum(
            abs(float(pos.get("net_size", 0.0))) * float(pos.get("avg_entry_price", 0.0))
            for pos in open_positions
        )
        
        # Get closed positions this hour (within the hour range)
        closed_this_hour = []
        for entry in tracker.snapshot_closed_positions(limit=0):
            closed_at = entry.get("closed_at", 0)
            if isinstance(closed_at, float):
                closed_at = int(closed_at)
            if start_timestamp <= closed_at < end_timestamp:
                closed_this_hour.append(entry)
        
        pnl_emoji = "✅" if hourly_pnl >= 0 else "❌"
        pnl_sign = "+" if hourly_pnl >= 0 else ""
        
        hour_str = hour_start.strftime("%I:%M %p")
        hour_end_str = hour_end.strftime("%I:%M %p")
        
        # Calculate today's total PnL from hourly breakdown
        today_total_pnl = sum(today_hourly_pnl.values()) if today_hourly_pnl else 0.0
        today_total_emoji = "✅" if today_total_pnl >= 0 else "❌"
        today_total_sign = "+" if today_total_pnl >= 0 else ""
        
        # Debug logging
        logger.info(f"/pnl command: hour={hour_str}-{hour_end_str}, "
                   f"hourly_pnl={hourly_pnl:.2f}, stats_trades={stats['trade_count']}, "
                   f"closed_this_hour={len(closed_this_hour)}, "
                   f"total_closed={closed_count}, open_positions={len(open_positions)}, "
                   f"peak_exposure={peak_concurrent_exposure:.2f}, live_exposure={live_concurrent_exposure:.2f}")
        
        message = (
            f"💰 Profit & Loss - Current Hour\n"
            f"⏰ {hour_str} - {hour_end_str}\n\n"
            f"{pnl_emoji} Hourly PnL: {pnl_sign}${hourly_pnl:.2f}\n"
            f"📊 Trades This Hour: {stats['trade_count']}\n"
            f"📉 Closed Positions: {len(closed_this_hour)}\n"
            f"💰 Volume: ${stats['volume']:.2f}\n\n"
            f"💎 Peak Concurrent Exposure: ${peak_concurrent_exposure:.2f}\n"
            f"💎 Live Concurrent Exposure: ${live_concurrent_exposure:.2f}\n\n"
            f"📈 Open Positions: {len(open_positions)}\n"
            f"📉 Total Closed Trades: {closed_count}\n\n"
            f"📅 *Today's Hourly PnL:*\n"
        )
        
        # Show hourly breakdown for today (sorted by hour)
        if today_hourly_pnl:
            # Sort hours chronologically
            sorted_hours = sorted(today_hourly_pnl.items(), key=lambda x: datetime.strptime(x[0], "%I:%M %p").hour)
            for hour_label, hour_pnl in sorted_hours:
                hour_emoji = "✅" if hour_pnl >= 0 else "❌"
                hour_sign = "+" if hour_pnl >= 0 else ""
                message += f"{hour_emoji} {hour_label}: {hour_sign}${hour_pnl:.2f}\n"
            
            message += f"\n{today_total_emoji} *Today Total:* {today_total_sign}${today_total_pnl:.2f}"
        else:
            message += "⚠️ Unavailable"
        
        # If no data, show helpful message
        if stats['trade_count'] == 0 and len(closed_this_hour) == 0 and hourly_pnl == 0:
            message += (
                f"\n\n⚠️ No trades found for this hour.\n"
                f"Try /backfill to refresh data from Polymarket."
            )
        
        if update.message:
            await update.message.reply_text(message, parse_mode='Markdown')
    
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
            f"💎 Peak Concurrent Exposure: ${stats.get('peak_concurrent_exposure', 0.0):.2f}\n"
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
    
    async def _calculate_end_of_hour_pnl(self, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Calculate and send total PnL summary at end of hour (:59:59).
        Also checks for resolved markets and applies resolutions with resolved prices."""
        if not self.enabled or not self.application:
            return
        
        logger.info("📊 Calculating end-of-hour PnL summary...")
        
        # Calculate the hour that just ended
        now = datetime.now()
        hour_end = now.replace(minute=0, second=0, microsecond=0)
        hour_start = hour_end - timedelta(hours=1)
        start_timestamp = int(hour_start.timestamp())
        end_timestamp = int(hour_end.timestamp())
        
        # Get all active users
        with self._lock:
            active_chats_copy = list(self.active_chats)
        
        for chat_id in active_chats_copy:
            try:
                tracker = self.get_user_tracker(chat_id)
                
                # 1. Calculate realized PnL from trades (closed positions) in this hour
                stats = tracker.get_hourly_stats(start_timestamp, end_timestamp)
                traded_profit = stats.get('pnl', 0.0)  # Realized PnL from closed trades
                
                # 2. Check for resolved markets and apply resolutions with resolved prices
                resolution_pnl = 0.0
                with tracker._lock:
                    open_positions = tracker.open_positions.copy()
                
                # Group positions by market_id
                markets_with_positions = {}
                for token_id, pos in open_positions.items():
                    market_id = pos.get("market_id")
                    if market_id:
                        if market_id not in markets_with_positions:
                            markets_with_positions[market_id] = []
                        markets_with_positions[market_id].append((token_id, pos))
                
                # Check each market for resolution
                from polymarket_copy_bot import get_market_tokens, get_market_prices, parse_resolution_time_from_title
                for market_id, positions in markets_with_positions.items():
                    if not positions:
                        continue
                    
                    # Get market title from first position
                    title = positions[0][1].get("title", "")
                    if not title:
                        continue
                    
                    # Check if market has resolved
                    resolution_time = parse_resolution_time_from_title(title)
                    if resolution_time and resolution_time <= now:
                        logger.info(f"✅ Market {market_id} has resolved (title: {title}, time: {resolution_time})")
                        
                        # Get market tokens (Up and Down)
                        market_tokens = get_market_tokens(market_id)
                        if not market_tokens:
                            # Try to get from position token_id
                            position_token_id = positions[0][0]
                            from polymarket_copy_bot import get_market_tokens_by_token_id
                            market_tokens = get_market_tokens_by_token_id(position_token_id)
                        
                        if market_tokens:
                            # Fetch resolved prices for both Up and Down tokens
                            up_token = market_tokens.get("UP") or market_tokens.get("Up")
                            down_token = market_tokens.get("DOWN") or market_tokens.get("Down")
                            
                            token_ids_to_fetch = []
                            if up_token:
                                token_ids_to_fetch.append(up_token)
                            if down_token:
                                token_ids_to_fetch.append(down_token)
                            
                            if token_ids_to_fetch:
                                all_prices = get_market_prices(token_ids_to_fetch)
                                
                                # Get resolved prices
                                up_price = None
                                down_price = None
                                
                                if up_token and up_token in all_prices:
                                    up_prices = all_prices[up_token]
                                    up_price = up_prices.get("BUY", 0.0) or up_prices.get("SELL", 0.0)
                                
                                if down_token and down_token in all_prices:
                                    down_prices = all_prices[down_token]
                                    down_price = down_prices.get("BUY", 0.0) or down_prices.get("SELL", 0.0)
                                
                                # Determine winning outcome: highest price wins (closest to 1.0)
                                winning_outcome = None
                                resolved_price = None
                                
                                if up_price is not None and down_price is not None:
                                    # Compare prices directly - highest price wins
                                    if up_price > down_price:
                                        winning_outcome = "Up"
                                        resolved_price = up_price
                                    elif down_price > up_price:
                                        winning_outcome = "Down"
                                        resolved_price = down_price
                                    else:
                                        # Equal prices - use the higher one (shouldn't happen but handle it)
                                        winning_outcome = "Up"  # Default to Up if equal
                                        resolved_price = up_price
                                elif up_price is not None:
                                    # Only Up price available - use it if >= 0.5
                                    if up_price >= 0.5:
                                        winning_outcome = "Up"
                                        resolved_price = up_price
                                elif down_price is not None:
                                    # Only Down price available - use it if >= 0.5
                                    if down_price >= 0.5:
                                        winning_outcome = "Down"
                                        resolved_price = down_price
                                
                                if winning_outcome and resolved_price:
                                    resolution_timestamp = int(resolution_time.timestamp())
                                    try:
                                        market_resolution_pnl = tracker.apply_resolution(
                                            market_id,
                                            winning_outcome,
                                            resolution_timestamp,
                                            resolved_price
                                        )
                                        resolution_pnl += market_resolution_pnl
                                        logger.info(f"✅ Applied resolution to market {market_id}: {winning_outcome} @ ${resolved_price:.4f}, PnL: ${market_resolution_pnl:.2f}")
                                    except Exception as e:
                                        logger.error(f"❌ ERROR applying resolution to market {market_id}: {type(e).__name__}: {e}")
                
                # 3. Calculate unrealized PnL from remaining open positions
                with tracker._lock:
                    open_positions = tracker.open_positions.copy()
                
                total_unrealized = 0.0
                positions_detail = []
                
                if open_positions:
                    # Collect all token IDs from open positions
                    token_ids = []
                    for token_id in open_positions.keys():
                        token_ids.append(token_id)
                    
                    # Fetch current prices for all tokens
                    from polymarket_copy_bot import get_market_prices
                    all_prices = get_market_prices(token_ids)
                    
                    # Extract current prices (use BUY price, fallback to SELL)
                    current_prices = {}
                    for token_id in token_ids:
                        if token_id in all_prices:
                            prices = all_prices[token_id]
                            buy_price = prices.get("BUY", 0.0)
                            sell_price = prices.get("SELL", 0.0)
                            current_price = buy_price if buy_price > 0 else sell_price
                            if current_price > 0:
                                current_prices[token_id] = current_price
                    
                    if current_prices:
                        # Calculate unrealized PnL
                        pnl_data = tracker.calculate_unrealized_pnl(current_prices)
                        total_unrealized = pnl_data["total_unrealized_pnl"]
                        positions_detail = pnl_data["positions"]
                
                # 4. Calculate total PnL (traded + resolution + unrealized)
                total_pnl = traded_profit + resolution_pnl + total_unrealized
                
                # Build message
                hour_str = hour_start.strftime("%I:%M %p")
                hour_end_str = hour_end.strftime("%I:%M %p")
                
                traded_emoji = "✅" if traded_profit >= 0 else "❌"
                traded_sign = "+" if traded_profit >= 0 else ""
                
                unrealized_emoji = "✅" if total_unrealized >= 0 else "❌"
                unrealized_sign = "+" if total_unrealized >= 0 else ""
                
                total_emoji = "✅" if total_pnl >= 0 else "❌"
                total_sign = "+" if total_pnl >= 0 else ""
                
                message = (
                    f"💰 *End-of-Hour PnL Summary*\n"
                    f"⏰ {hour_str} - {hour_end_str}\n\n"
                    f"📈 *Traded Profit:* {traded_emoji} {traded_sign}${traded_profit:.2f}\n"
                    f"🎲 *Polymarket Bet PnL:* {unrealized_emoji} {unrealized_sign}${total_unrealized:.2f}\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"💰 *Total PnL:* {total_emoji} {total_sign}${total_pnl:.2f}\n\n"
                )
                
                # Add open positions details if any
                if positions_detail:
                    message += f"📊 *Open Positions:* {len(positions_detail)}\n\n"
                    sorted_positions = sorted(positions_detail, key=lambda x: abs(x["unrealized_pnl"]), reverse=True)
                    for pos in sorted_positions[:5]:  # Show top 5
                        coin = pos["coin_name"]
                        outcome = pos["outcome"]
                        size = pos["size"]
                        entry_price = pos["entry_price"]
                        current_price = pos["current_price"]
                        unrealized = pos["unrealized_pnl"]
                        
                        pos_emoji = "✅" if unrealized >= 0 else "❌"
                        pos_sign = "+" if unrealized >= 0 else ""
                        direction = "🟢" if size > 0 else "🔴"
                        
                        message += (
                            f"{pos_emoji} {coin} {outcome}\n"
                            f"   {direction} {abs(size):.1f} @ ${entry_price:.4f} → ${current_price:.4f}\n"
                            f"   {pos_sign}${unrealized:.2f}\n\n"
                        )
                    
                    if len(sorted_positions) > 5:
                        message += f"... and {len(sorted_positions) - 5} more positions"
                
                await self.send_message_async(message, chat_id, parse_mode='Markdown')
                logger.info(f"Sent end-of-hour PnL summary to chat_id={chat_id}: Traded=${traded_profit:.2f}, Unrealized=${total_unrealized:.2f}, Total=${total_pnl:.2f}")
                
            except Exception as e:
                logger.warning(f"Failed to calculate end-of-hour PnL for chat {chat_id}: {e}", exc_info=True)
                self.active_chats.discard(chat_id)
    
    async def _handle_backfill(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /backfill command - reset current hour's data and refresh with accurate trades."""
        try:
            logger.info("=" * 80)
            logger.info("🔵 BACKFILL COMMAND RECEIVED - STARTING")
            logger.info("=" * 80)
            logger.info(f"Update object: {update}")
            logger.info(f"Update.effective_chat: {update.effective_chat}")
            logger.info(f"Update.message: {update.message}")
            
            if not update.effective_chat:
                logger.error("❌ ERROR: No effective chat in update - cannot process backfill")
                return
            
            chat_id = update.effective_chat.id
            logger.info(f"✅ Chat ID extracted: {chat_id}")
            logger.info(f"✅ Adding chat_id {chat_id} to active_chats")
            self.active_chats.add(chat_id)
            logger.info(f"✅ Active chats now: {list(self.active_chats)}")
            
            # Send immediate response
            status_msg = None
            try:
                if update.message:
                    logger.info("📤 Sending initial 'Searching...' message to Telegram")
                    status_msg = await update.message.reply_text("🔍 Searching for trades... Please wait...")
                    logger.info("✅ Initial message sent successfully")
                else:
                    logger.warning("⚠️  No update.message available - cannot send initial response")
            except Exception as e:
                logger.error(f"❌ ERROR sending initial message: {type(e).__name__}: {e}")
                logger.exception("Full traceback:")
                # Continue anyway
        
            # Calculate current hour (closest hour)
            # If it's 2:23 PM, we want 2:00 PM - 3:00 PM (the current hour)
            logger.info("📅 Calculating current hour range...")
            now = datetime.now()
            hour_start = now.replace(minute=0, second=0, microsecond=0)
            hour_end = hour_start + timedelta(hours=1)  # End of current hour
            start_timestamp = int(hour_start.timestamp())
            end_timestamp = int(hour_end.timestamp())
            
            logger.info(f"✅ Current time: {now}")
            logger.info(f"✅ Hour range: {hour_start} to {hour_end}")
            logger.info(f"✅ Timestamp range: {start_timestamp} to {end_timestamp}")
        
            # Update status message
            try:
                if update.message and status_msg:
                    logger.info("📤 Updating status message: 'Resetting current hour's data...'")
                    await status_msg.edit_text(
                        f"🔄 Resetting current hour's data...\n"
                        f"⏰ Hour: {hour_start.strftime('%I:%M %p')} - {hour_end.strftime('%I:%M %p')}\n\n"
                        f"⏳ This may take a moment..."
                    )
                    logger.info("✅ Status message updated")
            except Exception as e:
                logger.error(f"❌ ERROR updating status message: {type(e).__name__}: {e}")
                logger.exception("Full traceback:")
            
            # Get current tracker
            logger.info(f"📊 Getting user tracker for chat_id={chat_id}...")
            tracker = self.get_user_tracker(chat_id)
            logger.info(f"✅ Got tracker: {len(tracker.trade_history)} trades in history, "
                       f"{len(tracker.open_positions)} open positions, "
                       f"{len(tracker.closed_positions)} closed positions")
        
            # Remove all data from the current hour
            logger.info("🗑️  Removing all data from current hour...")
            with tracker._lock:
                initial_trade_count = len(tracker.trade_history)
                initial_closed_count = len(tracker.closed_positions)
                initial_open_count = len(tracker.open_positions)
                logger.info(f"📊 Initial state: {initial_trade_count} trades, {initial_closed_count} closed, {initial_open_count} open")
                
                # Remove trades from current hour
                logger.info(f"🔍 Filtering trades: looking for timestamps in range [{start_timestamp}, {end_timestamp})")
                tracker.trade_history = [
                    t for t in tracker.trade_history
                    if not (start_timestamp <= t.get("timestamp", 0) < end_timestamp)
                ]
                removed_trades = initial_trade_count - len(tracker.trade_history)
                logger.info(f"✅ Removed {removed_trades} trades from current hour (had {initial_trade_count}, now {len(tracker.trade_history)})")
                
                # Remove closed positions that closed in current hour
                logger.info(f"🔍 Filtering closed positions: looking for closed_at in range [{start_timestamp}, {end_timestamp})")
                tracker.closed_positions = [
                    entry for entry in tracker.closed_positions
                    if not (start_timestamp <= entry.get("closed_at", 0) < end_timestamp)
                ]
                removed_closed = initial_closed_count - len(tracker.closed_positions)
                logger.info(f"✅ Removed {removed_closed} closed positions from current hour (had {initial_closed_count}, now {len(tracker.closed_positions)})")
                
                # Remove open positions that were opened in current hour
                # (positions opened before current hour but still open should remain)
                # Use opened_at if available, otherwise fall back to last_update
                logger.info(f"🔍 Filtering open positions: removing those opened in range [{start_timestamp}, {end_timestamp})")
                tracker.open_positions = {
                    token_id: pos for token_id, pos in tracker.open_positions.items()
                    if pos.get("opened_at", pos.get("last_update", 0)) < start_timestamp
                }
                removed_open = initial_open_count - len(tracker.open_positions)
                logger.info(f"✅ Removed {removed_open} open positions from current hour (had {initial_open_count}, now {len(tracker.open_positions)})")
                
                # Recalculate PnL from remaining closed positions
                old_pnl = tracker.realized_pnl
                tracker.realized_pnl = sum(
                    float(entry.get("realized_pnl", 0.0))
                    for entry in tracker.closed_positions
                )
                logger.info(f"✅ Recalculated PnL: {old_pnl:.2f} -> {tracker.realized_pnl:.2f}")
            logger.info("✅ Finished removing current hour's data")
        
            # Update status message
            try:
                if update.message and status_msg:
                    logger.info("📤 Updating status message: 'Fetching all trades...'")
                    await status_msg.edit_text(
                        f"📥 Fetching all trades from current hour...\n"
                        f"⏰ {hour_start.strftime('%I:%M %p')} - {hour_end.strftime('%I:%M %p')}\n\n"
                        f"⏳ Fetching from Polymarket (this may take a while for many trades)..."
                    )
                    logger.info("✅ Status message updated")
            except Exception as e:
                logger.error(f"❌ ERROR updating status message: {type(e).__name__}: {e}")
            
            logger.info(f"🌐 Fetching trades from Polymarket API for wallet {TARGET_WALLET}")
            logger.info(f"📅 Time range: {start_timestamp} ({hour_start}) to {end_timestamp} ({hour_end})")
            logger.info(f"🔍 Calling fetch_trades_in_range({TARGET_WALLET}, {start_timestamp}, {end_timestamp})...")
            
            # Fetch all trades from the current hour
            try:
                trades = fetch_trades_in_range(TARGET_WALLET, start_timestamp, end_timestamp)
                logger.info(f"✅ fetch_trades_in_range returned {len(trades)} trades")
                if trades:
                    first_ts = getattr(trades[0], 'timestamp', 'N/A')
                    last_ts = getattr(trades[-1], 'timestamp', 'N/A')
                    logger.info(f"📊 First trade timestamp: {first_ts}")
                    logger.info(f"📊 Last trade timestamp: {last_ts}")
            except Exception as e:
                logger.error(f"❌ ERROR in fetch_trades_in_range: {type(e).__name__}: {e}")
                logger.exception("Full traceback:")
                trades = []
        
            # Immediately show how many trades were found
            try:
                if update.message:
                    logger.info(f"📤 Sending trade count message: Found {len(trades)} trades")
                    if status_msg:
                        await status_msg.edit_text(
                            f"📊 Found {len(trades)} trades in the hour!\n"
                            f"⏰ {hour_start.strftime('%I:%M %p')} - {hour_end.strftime('%I:%M %p')}\n\n"
                            f"🔄 Processing trades with hedging..."
                        )
                    else:
                        await update.message.reply_text(
                            f"📊 Found {len(trades)} trades in the hour!\n"
                            f"⏰ {hour_start.strftime('%I:%M %p')} - {hour_end.strftime('%I:%M %p')}\n\n"
                            f"🔄 Processing trades with hedging..."
                        )
                    logger.info("✅ Trade count message sent")
            except Exception as e:
                logger.error(f"❌ ERROR sending trade count message: {type(e).__name__}: {e}")
            
            if not trades:
                logger.warning(f"⚠️  No trades found in hour {hour_start} - {hour_end}")
                try:
                    if update.message and status_msg:
                        await status_msg.edit_text(
                            f"📭 No trades found in the hour {hour_start.strftime('%I:%M %p')} - {hour_end.strftime('%I:%M %p')}.\n"
                            "The target wallet may not have traded during this period.\n\n"
                            "✅ Current hour's data has been reset."
                        )
                except Exception as e:
                    logger.error(f"❌ ERROR sending 'no trades' message: {type(e).__name__}: {e}")
                # Save the reset state
                logger.info("💾 Saving user tracker state (no trades found)...")
                self.save_user_tracker(chat_id)
                logger.info("✅ Backfill complete (no trades found)")
                logger.info("=" * 80)
                return
        
            logger.info(f"✅ Found {len(trades)} trades to process")
            
            # Process each trade in chronological order
            processed_count = 0
            skipped_count = 0
            logger.info(f"🔄 Starting to process {len(trades)} trades...")
            
            for idx, trade in enumerate(trades):
                if idx % 10 == 0:  # Log every 10th trade
                    logger.info(f"📊 Processing trade {idx + 1}/{len(trades)}...")
                
                logger.debug(f"Processing trade {idx + 1}/{len(trades)}: {trade.outcome} {trade.side} "
                            f"{trade.size:.2f} @ ${trade.price:.4f} (tx: {trade.transaction_hash[:16]}...)")
                
                try:
                    # Log all trades (even filtered ones) for tracking
                    logger.info(f"📊 Trade {idx + 1}/{len(trades)}: {trade.outcome} {trade.side} "
                              f"{trade.size:.2f} @ ${trade.price:.4f} (tx: {trade.transaction_hash[:16]}...)")
                    
                    # Check if we should copy this trade
                    if not should_copy_trade(trade):
                        logger.info(f"⚠️  Trade {idx + 1} filtered out by should_copy_trade - NOT copying")
                        skipped_count += 1
                        continue
                    
                    # Calculate our size (using current risk settings)
                    my_size = compute_my_size(trade, RISK_MULTIPLIER, MAX_TRADE_USDC)
                    logger.debug(f"Trade {idx + 1} size: original={trade.size:.2f}, my_size={my_size:.2f}")
                    
                    if my_size <= 0:
                        logger.debug(f"Trade {idx + 1} skipped: my_size <= 0")
                        skipped_count += 1
                        continue
                    
                    # Apply trade to user's tracker (simulate, don't place real orders)
                    # This will automatically handle hedging when opposite positions are detected
                    logger.debug(f"Applying trade {idx + 1} to tracker...")
                    position_events = tracker.apply_trade(trade, my_size)
                    logger.debug(f"Trade {idx + 1} applied: {len(position_events)} position events")
                    
                    # Notify user of position events (including hedging/closing)
                    if position_events:
                        logger.debug(f"Trade {idx + 1} generated events: {[e.event_type for e in position_events]}")
                        try:
                            self.notify_events(position_events, chat_id=chat_id)
                        except Exception as e:
                            logger.error(f"❌ ERROR notifying events for trade {idx + 1}: {type(e).__name__}: {e}")
                    
                    processed_count += 1
                except Exception as e:
                    logger.error(f"❌ ERROR processing trade {idx + 1}: {type(e).__name__}: {e}")
                    logger.exception("Full traceback:")
                    skipped_count += 1
            
            logger.info(f"✅ Processing complete: {processed_count} processed, {skipped_count} skipped")
        
            # Save user state after processing all trades
            logger.info("💾 Saving user tracker state after processing trades...")
            try:
                self.save_user_tracker(chat_id)
                logger.info("✅ User tracker state saved")
            except Exception as e:
                logger.error(f"❌ ERROR saving user tracker: {type(e).__name__}: {e}")
                logger.exception("Full traceback:")
            
            # Get updated stats for the hour (includes hedging PnL)
            logger.info("📊 Calculating hourly stats...")
            try:
                stats = tracker.get_hourly_stats(start_timestamp, end_timestamp)
                logger.info(f"✅ Hourly stats: pnl={stats['pnl']:.2f}, volume={stats['volume']:.2f}, "
                           f"trades={stats['trade_count']}, peak_exposure={stats.get('peak_concurrent_exposure', 0.0):.2f}")
            except Exception as e:
                logger.error(f"❌ ERROR calculating hourly stats: {type(e).__name__}: {e}")
                logger.exception("Full traceback:")
                stats = {
                    'pnl': 0.0,
                    'volume': 0.0,
                    'trade_count': 0,
                    'peak_concurrent_exposure': 0.0
                }
            
            # Send final summary
            try:
                if update.message and status_msg:
                    pnl_emoji = "✅" if stats['pnl'] >= 0 else "❌"
                    pnl_sign = "+" if stats['pnl'] >= 0 else ""
                    await status_msg.edit_text(
                        f"✅ *Backfill Complete!*\n\n"
                        f"⏰ Hour: {hour_start.strftime('%I:%M %p')} - {hour_end.strftime('%I:%M %p')}\n\n"
                        f"📊 *Results:*\n"
                        f"   • Trades Found: {len(trades)}\n"
                        f"   • Processed: {processed_count}\n"
                        f"   • Skipped: {skipped_count}\n\n"
                        f"{pnl_emoji} *Hourly PnL:* {pnl_sign}${stats['pnl']:.2f}\n"
                        f"💵 *Volume:* ${stats['volume']:.2f}\n"
                        f"💎 *Peak Exposure:* ${stats.get('peak_concurrent_exposure', 0.0):.2f}"
                    )
            except Exception as e:
                logger.error(f"❌ ERROR sending final summary: {type(e).__name__}: {e}")
            
            logger.info("=" * 80)
            logger.info("✅ BACKFILL COMMAND COMPLETED SUCCESSFULLY")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"❌ FATAL ERROR in backfill command: {type(e).__name__}: {e}")
            logger.exception("Full traceback:")
            try:
                if update.message:
                    await update.message.reply_text(
                        f"❌ Error during backfill: {str(e)}\n"
                        "Please check the logs for details."
                    )
            except:
                pass
    
    async def _handle_backfill_all(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /backfillall command - reset last 24 hours (12am today to now) and refresh with accurate trades."""
        try:
            logger.info("=" * 80)
            logger.info("🔵 BACKFILL ALL COMMAND RECEIVED - STARTING (24 HOURS)")
            logger.info("=" * 80)
            
            if not update.effective_chat:
                logger.error("❌ ERROR: No effective chat in update - cannot process backfill all")
                return
            
            chat_id = update.effective_chat.id
            logger.info(f"✅ Chat ID extracted: {chat_id}")
            self.active_chats.add(chat_id)
            
            # Send immediate response
            status_msg = None
            try:
                if update.message:
                    logger.info("📤 Sending initial 'Searching...' message to Telegram")
                    status_msg = await update.message.reply_text("🔍 Backfilling last 24 hours... This may take a while...")
                    logger.info("✅ Initial message sent successfully")
            except Exception as e:
                logger.error(f"❌ ERROR sending initial message: {type(e).__name__}: {e}")
            
            # Calculate 24-hour period: from 12am today to now
            logger.info("📅 Calculating 24-hour period (12am today to now)...")
            now = datetime.now()
            day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            start_timestamp = int(day_start.timestamp())
            end_timestamp = int(now.timestamp())
            
            logger.info(f"✅ Time range: {day_start} to {now}")
            logger.info(f"✅ Timestamp range: {start_timestamp} to {end_timestamp}")
        
            # Update status message
            try:
                if update.message and status_msg:
                    await status_msg.edit_text(
                        f"🔄 Resetting last 24 hours of data...\n"
                        f"📅 From: {day_start.strftime('%B %d, %Y %I:%M %p')}\n"
                        f"📅 To: {now.strftime('%B %d, %Y %I:%M %p')}\n\n"
                        f"⏳ This may take a while..."
                    )
            except Exception as e:
                logger.error(f"❌ ERROR updating status message: {type(e).__name__}: {e}")
            
            # Get current tracker
            logger.info(f"📊 Getting user tracker for chat_id={chat_id}...")
            tracker = self.get_user_tracker(chat_id)
            logger.info(f"✅ Got tracker: {len(tracker.trade_history)} trades in history, "
                       f"{len(tracker.open_positions)} open positions, "
                       f"{len(tracker.closed_positions)} closed positions")
        
            # Remove all data from the last 24 hours
            logger.info("🗑️  Removing all data from last 24 hours...")
            with tracker._lock:
                initial_trade_count = len(tracker.trade_history)
                initial_closed_count = len(tracker.closed_positions)
                initial_open_count = len(tracker.open_positions)
                logger.info(f"📊 Initial state: {initial_trade_count} trades, {initial_closed_count} closed, {initial_open_count} open")
                
                # Remove trades from last 24 hours
                tracker.trade_history = [
                    t for t in tracker.trade_history
                    if not (start_timestamp <= t.get("timestamp", 0) < end_timestamp)
                ]
                removed_trades = initial_trade_count - len(tracker.trade_history)
                logger.info(f"✅ Removed {removed_trades} trades from last 24 hours")
                
                # Remove closed positions that closed in last 24 hours
                tracker.closed_positions = [
                    entry for entry in tracker.closed_positions
                    if not (start_timestamp <= entry.get("closed_at", 0) < end_timestamp)
                ]
                removed_closed = initial_closed_count - len(tracker.closed_positions)
                logger.info(f"✅ Removed {removed_closed} closed positions from last 24 hours")
                
                # Remove open positions that were opened in last 24 hours
                tracker.open_positions = {
                    token_id: pos for token_id, pos in tracker.open_positions.items()
                    if pos.get("opened_at", pos.get("last_update", 0)) < start_timestamp
                }
                removed_open = initial_open_count - len(tracker.open_positions)
                logger.info(f"✅ Removed {removed_open} open positions from last 24 hours")
                
                # Recalculate PnL from remaining closed positions
                old_pnl = tracker.realized_pnl
                tracker.realized_pnl = sum(
                    float(entry.get("realized_pnl", 0.0))
                    for entry in tracker.closed_positions
                )
                logger.info(f"✅ Recalculated PnL: {old_pnl:.2f} -> {tracker.realized_pnl:.2f}")
            logger.info("✅ Finished removing last 24 hours of data")
        
            # Update status message
            try:
                if update.message and status_msg:
                    await status_msg.edit_text(
                        f"📥 Fetching all trades from last 24 hours...\n"
                        f"📅 {day_start.strftime('%B %d, %Y %I:%M %p')} - {now.strftime('%B %d, %Y %I:%M %p')}\n\n"
                        f"⏳ Fetching from Polymarket (this may take several minutes for many trades)..."
                    )
            except Exception as e:
                logger.error(f"❌ ERROR updating status message: {type(e).__name__}: {e}")
            
            logger.info(f"🌐 Fetching trades from Polymarket API for wallet {TARGET_WALLET}")
            logger.info(f"📅 Time range: {start_timestamp} ({day_start}) to {end_timestamp} ({now})")
            
            # Fetch all trades from the last 24 hours
            try:
                trades = fetch_trades_in_range(TARGET_WALLET, start_timestamp, end_timestamp)
                logger.info(f"✅ fetch_trades_in_range returned {len(trades)} trades")
            except Exception as e:
                logger.error(f"❌ ERROR in fetch_trades_in_range: {type(e).__name__}: {e}")
                logger.exception("Full traceback:")
                trades = []
        
            # Show how many trades were found
            try:
                if update.message and status_msg:
                    await status_msg.edit_text(
                        f"📊 Found {len(trades)} trades in the last 24 hours!\n"
                        f"📅 {day_start.strftime('%B %d, %Y %I:%M %p')} - {now.strftime('%B %d, %Y %I:%M %p')}\n\n"
                        f"🔄 Processing trades with hedging..."
                    )
            except Exception as e:
                logger.error(f"❌ ERROR sending trade count message: {type(e).__name__}: {e}")
            
            if not trades:
                logger.warning(f"⚠️  No trades found in last 24 hours")
                try:
                    if update.message and status_msg:
                        await status_msg.edit_text(
                            f"📭 No trades found in the last 24 hours.\n"
                            "The target wallet may not have traded during this period.\n\n"
                            "✅ Last 24 hours' data has been reset."
                        )
                except Exception as e:
                    logger.error(f"❌ ERROR sending 'no trades' message: {type(e).__name__}: {e}")
                self.save_user_tracker(chat_id)
                logger.info("✅ Backfill all complete (no trades found)")
                logger.info("=" * 80)
                return
        
            logger.info(f"✅ Found {len(trades)} trades to process")
            
            # Process each trade in chronological order
            processed_count = 0
            skipped_count = 0
            logger.info(f"🔄 Starting to process {len(trades)} trades...")
            
            for idx, trade in enumerate(trades):
                if idx % 100 == 0:  # Log every 100th trade for 24-hour backfill
                    logger.info(f"📊 Processing trade {idx + 1}/{len(trades)}... (processed: {processed_count}, skipped: {skipped_count})")
                    # Update status periodically (less frequently to avoid rate limits)
                    try:
                        if update.message and status_msg and idx > 0 and idx % 200 == 0:
                            await status_msg.edit_text(
                                f"🔄 Processing trades...\n"
                                f"📊 Progress: {idx + 1}/{len(trades)} trades\n"
                                f"✅ Processed: {processed_count}\n"
                                f"⏭️  Skipped: {skipped_count}\n\n"
                                f"⏳ This may take a few minutes..."
                            )
                    except Exception as e:
                        logger.debug(f"Could not update status message: {e}")
                        pass
                
                try:
                    if not should_copy_trade(trade):
                        skipped_count += 1
                        continue
                    
                    my_size = compute_my_size(trade, RISK_MULTIPLIER, MAX_TRADE_USDC)
                    if my_size <= 0:
                        skipped_count += 1
                        continue
                    
                    position_events = tracker.apply_trade(trade, my_size)
                    # Skip notifications during backfill to avoid performance issues
                    # We'll show a summary at the end instead
                    # if position_events:
                    #     try:
                    #         self.notify_events(position_events, chat_id=chat_id)
                    #     except Exception as e:
                    #         logger.error(f"❌ ERROR notifying events: {type(e).__name__}: {e}")
                    
                    processed_count += 1
                except Exception as e:
                    logger.error(f"❌ ERROR processing trade {idx + 1}: {type(e).__name__}: {e}")
                    skipped_count += 1
            
            logger.info(f"✅ Processing complete: {processed_count} processed, {skipped_count} skipped")
        
            # Fetch and apply market resolutions for the period
            logger.info("🔍 Fetching market resolutions for the period...")
            try:
                if update.message and status_msg:
                    await status_msg.edit_text(
                        f"🔍 Fetching market resolutions...\n"
                        f"📊 Processed: {processed_count} trades\n"
                        f"⏳ Checking for resolved markets..."
                    )
            except:
                pass
            
            # Check for resolved markets by parsing market titles for resolution times
            # Markets like "Bitcoin Up or Down - December 2, 5PM ET" resolve at the specified time
            # Polymarket uses 0-100 scale (cents), so winner is closest to 100
            # 
            # Note: Official resolution announcements (e.g., from Polymarket Discord) could provide
            # more reliable resolution data than parsing titles and checking prices. Consider integrating
            # Discord API or webhook for real-time resolution notifications.
            logger.info("🔍 Checking for resolved markets by parsing market titles...")
            resolution_count = 0
            total_resolution_pnl = 0.0
            
            # Create detailed resolution log file
            resolution_log_file = Path("resolution_log.json")
            resolution_log = {
                "timestamp": int(now.timestamp()),
                "period": {
                    "start": start_timestamp,
                    "end": end_timestamp,
                    "start_str": day_start.isoformat(),
                    "end_str": now.isoformat()
                },
                "markets_checked": [],
                "resolutions_applied": []
            }
            
            # Collect all unique markets from trades with their titles
            market_info = {}  # market_id -> {"title": str, "trades": [Trade]}
            for trade in trades:
                market_id = trade.market_id
                if market_id:
                    if market_id not in market_info:
                        market_info[market_id] = {"title": trade.title, "trades": []}
                    market_info[market_id]["trades"].append(trade)
            
            logger.info(f"📊 Found {len(market_info)} unique markets to check")
            
            # Also check all open positions for markets we might have missed
            # (markets that resolved but we don't have trades for in this period)
            open_positions_snapshot = tracker.snapshot_open_positions()
            for pos in open_positions_snapshot:
                market_id = pos.get("market_id", "")
                if market_id and market_id not in market_info:
                    # We have an open position in a market we didn't see trades for
                    # This could be a resolved market - check it
                    title = pos.get("title", "")
                    if title:
                        market_info[market_id] = {"title": title, "trades": []}
                        logger.info(f"📊 Found market {market_id} from open position: {title}")
            
            logger.info(f"📊 Total markets to check (including from open positions): {len(market_info)}")
            
            # For each market, check if it has resolved
            resolved_markets = {}  # market_id -> {"winning_outcome": str, "resolution_time": datetime}
            
            for market_id, info in market_info.items():
                title = info.get("title", "")
                if not title:
                    continue
                
                # Get a sample trade to parse resolution time
                sample_trade = info["trades"][0] if info["trades"] else None
                if not sample_trade:
                    continue
                
                # Parse resolution time from title
                from polymarket_copy_bot import parse_resolution_time_from_title
                resolution_time = parse_resolution_time_from_title(title)
                if resolution_time and resolution_time < now:
                    # Market has resolved - need to determine winning outcome
                    logger.info(f"✅ Market {market_id} has resolved (title: {title}, time: {resolution_time})")
                    
                    # Determine winning outcome by checking final trade prices
                    # Polymarket uses 0-100 scale (cents), so winner is closest to 100 (1.00)
                    # At resolution, winning outcome should be near 100 cents, losing near 0 cents
                    up_trades = [t for t in info["trades"] if t.outcome.upper() == "UP"]
                    down_trades = [t for t in info["trades"] if t.outcome.upper() == "DOWN"]
                    
                    winning_outcome = None
                    resolved_price = None
                    resolution_ts = int(resolution_time.timestamp())
                    
                    # Log market details for debugging
                    market_log_entry = {
                        "market_id": market_id,
                        "title": title,
                        "resolution_time": resolution_time.isoformat(),
                        "resolution_timestamp": resolution_ts,
                        "up_trades_count": len(up_trades),
                        "down_trades_count": len(down_trades)
                    }
                    
                    # Fetch current prices from API (last-second price check)
                    # Highest price determines the winner
                    from polymarket_copy_bot import get_market_tokens, get_market_prices
                    market_tokens = get_market_tokens(market_id)
                    if not market_tokens:
                        # Try to get from a trade's token_id
                        if up_trades:
                            from polymarket_copy_bot import get_market_tokens_by_token_id
                            market_tokens = get_market_tokens_by_token_id(up_trades[0].token_id)
                        elif down_trades:
                            from polymarket_copy_bot import get_market_tokens_by_token_id
                            market_tokens = get_market_tokens_by_token_id(down_trades[0].token_id)
                    
                    up_price = None
                    down_price = None
                    
                    if market_tokens:
                        up_token = market_tokens.get("UP") or market_tokens.get("Up")
                        down_token = market_tokens.get("DOWN") or market_tokens.get("Down")
                        
                        token_ids_to_fetch = []
                        if up_token:
                            token_ids_to_fetch.append(up_token)
                        if down_token:
                            token_ids_to_fetch.append(down_token)
                        
                        if token_ids_to_fetch:
                            all_prices = get_market_prices(token_ids_to_fetch)
                            
                            if up_token and up_token in all_prices:
                                up_prices = all_prices[up_token]
                                up_price = up_prices.get("BUY", 0.0) or up_prices.get("SELL", 0.0)
                            
                            if down_token and down_token in all_prices:
                                down_prices = all_prices[down_token]
                                down_price = down_prices.get("BUY", 0.0) or down_prices.get("SELL", 0.0)
                    
                    # Determine winner: highest price wins (closest to 1.0)
                    if up_price is not None and down_price is not None:
                        if up_price > down_price:
                            winning_outcome = "Up"
                            resolved_price = up_price
                        elif down_price > up_price:
                            winning_outcome = "Down"
                            resolved_price = down_price
                        else:
                            # Equal prices - default to Up
                            winning_outcome = "Up"
                            resolved_price = up_price
                        
                        up_price_cents = int(up_price * 100)
                        down_price_cents = int(down_price * 100)
                        winner_price_cents = int(resolved_price * 100)
                        loser_price_cents = int((down_price if winning_outcome == "Up" else up_price) * 100)
                        
                        market_log_entry["final_prices"] = {
                            "up_price": up_price,
                            "up_price_cents": up_price_cents,
                            "down_price": down_price,
                            "down_price_cents": down_price_cents,
                            "winner": winning_outcome,
                            "winner_price_cents": winner_price_cents,
                            "loser_price_cents": loser_price_cents,
                            "resolved_price": resolved_price
                        }
                        
                        logger.info(f"📊 Market {market_id}: Current prices - Up: {up_price_cents}¢ (${up_price:.4f}), Down: {down_price_cents}¢ (${down_price:.4f}), Winner: {winning_outcome} ({winner_price_cents}¢) - Highest price wins")
                    elif up_price is not None:
                        # Only Up price available
                        if up_price >= 0.5:
                            winning_outcome = "Up"
                            resolved_price = up_price
                            up_price_cents = int(up_price * 100)
                            market_log_entry["final_prices"] = {
                                "up_price": up_price,
                                "up_price_cents": up_price_cents,
                                "down_price": 0.0,
                                "down_price_cents": 0,
                                "winner": winning_outcome,
                                "winner_price_cents": up_price_cents,
                                "loser_price_cents": 0,
                                "resolved_price": resolved_price
                            }
                    elif down_price is not None:
                        # Only Down price available
                        if down_price >= 0.5:
                            winning_outcome = "Down"
                            resolved_price = down_price
                            down_price_cents = int(down_price * 100)
                            market_log_entry["final_prices"] = {
                                "up_price": 0.0,
                                "up_price_cents": 0,
                                "down_price": down_price,
                                "down_price_cents": down_price_cents,
                                "winner": winning_outcome,
                                "winner_price_cents": down_price_cents,
                                "loser_price_cents": 0,
                                "resolved_price": resolved_price
                            }
                    else:
                        logger.warning(f"⚠️  Could not fetch prices for {market_id}, skipping resolution")
                        resolution_log["markets_checked"].append(market_log_entry)
                        continue
                    
                    if winning_outcome and resolved_price is not None:
                        resolved_markets[market_id] = {
                            "winning_outcome": winning_outcome,
                            "resolution_time": resolution_time,
                            "resolved_price": resolved_price
                        }
                        market_log_entry["winning_outcome"] = winning_outcome
                        market_log_entry["resolved"] = True
                        logger.info(f"✅ Determined winner for {market_id}: {winning_outcome} @ ${resolved_price:.4f} (resolution: {resolution_time})")
                    
                    resolution_log["markets_checked"].append(market_log_entry)
            
            logger.info(f"✅ Found {len(resolved_markets)} resolved markets")
            
            # Apply resolutions to ALL positions that were open at resolution time
            # Rule: If position closes BEFORE resolution, ignore it (no payout)
            #       If position closes AFTER resolution, it gets the payout
            #       If position is still open, it gets the payout
            
            for market_id, resolution_info in resolved_markets.items():
                winning_outcome = resolution_info["winning_outcome"]
                resolution_time = resolution_info["resolution_time"]
                resolved_price = resolution_info.get("resolved_price", 1.0)  # Use stored resolved_price or default to 1.0
                resolution_timestamp = int(resolution_time.timestamp())
                
                logger.info(f"🔍 Processing resolution for market {market_id} (winner: {winning_outcome} @ ${resolved_price:.4f}, time: {resolution_time})")
                
                # Find ALL positions that were open at resolution time
                positions_to_resolve = []
                
                # 1. Check currently open positions - only if opened BEFORE resolution
                open_positions = tracker.snapshot_open_positions()
                for pos in open_positions:
                    if pos.get("market_id") == market_id:
                        opened_at = pos.get("opened_at", pos.get("last_update", 0))
                        # Position was open at resolution if it was opened before resolution
                        if opened_at <= resolution_timestamp:
                            positions_to_resolve.append({
                                "type": "open",
                                "token_id": pos.get("token_id", ""),
                                "outcome": pos.get("outcome", ""),
                                "size": abs(float(pos.get("net_size", 0.0))),
                                "entry_price": float(pos.get("avg_entry_price", 0.0)),
                                "opened_at": opened_at,
                                "title": pos.get("title", ""),
                                "coin_name": pos.get("coin_name", "Unknown")
                            })
                
                # 2. Check closed positions - only if closed AFTER resolution
                # A position was open at resolution if: opened_at <= resolution_time < closed_at
                for closed_pos in tracker.closed_positions:
                    if closed_pos.get("market_id") == market_id:
                        opened_at = closed_pos.get("opened_at", 0)
                        closed_at = closed_pos.get("closed_at", 0)
                        # Position was open at resolution time (opened before, closed after)
                        if opened_at <= resolution_timestamp < closed_at:
                            outcome = closed_pos.get("outcome", "")
                            # Extract the actual outcome (might be "Up" or "Down" or "Up → Down")
                            actual_outcome = outcome.split(" → ")[0] if " → " in outcome else outcome
                            
                            positions_to_resolve.append({
                                "type": "closed",
                                "token_id": closed_pos.get("token_id", ""),
                                "outcome": actual_outcome,
                                "size": abs(float(closed_pos.get("size", closed_pos.get("entry_size", 0.0)))),
                                "entry_price": float(closed_pos.get("entry_price", 0.0)),
                                "opened_at": opened_at,
                                "closed_at": closed_at,
                                "title": closed_pos.get("title", ""),
                                "coin_name": closed_pos.get("coin_name", "Unknown")
                            })
                
                logger.info(f"📊 Found {len(positions_to_resolve)} positions open at resolution time for market {market_id}")
                
                # Separate open and closed positions
                open_positions_to_resolve = [p for p in positions_to_resolve if p["type"] == "open"]
                closed_positions_to_resolve = [p for p in positions_to_resolve if p["type"] == "closed"]
                
                market_resolution_pnl = 0.0
                
                # 1. Handle open positions using apply_resolution (handles state updates)
                if open_positions_to_resolve:
                    try:
                        open_pnl = tracker.apply_resolution(
                            market_id,
                            winning_outcome,
                            resolution_timestamp,
                            resolved_price
                        )
                        market_resolution_pnl += open_pnl
                        logger.info(f"✅ Applied resolution to {len(open_positions_to_resolve)} open positions: pnl=${open_pnl:.2f}")
                    except Exception as e:
                        logger.error(f"❌ ERROR applying resolution to open positions for {market_id}: {type(e).__name__}: {e}")
                
                # 2. Handle closed positions manually (they're already closed, just need resolution PnL)
                for pos_info in closed_positions_to_resolve:
                    outcome = pos_info["outcome"]
                    size = pos_info["size"]
                    entry_price = pos_info["entry_price"]
                    cost_basis = size * entry_price
                    token_id = pos_info["token_id"]
                    
                    # Determine if this position won
                    is_winner = (outcome.upper() == winning_outcome.upper())
                    
                    if is_winner:
                        # Winning position: payout based on resolved_price/entry_price ratio
                        if entry_price > 0:
                            payout = size * (resolved_price / entry_price)
                        else:
                            payout = size * resolved_price
                        pnl = payout - cost_basis
                    else:
                        # Losing position: pays $0 (full loss)
                        payout = 0.0
                        pnl = -cost_basis
                    
                    market_resolution_pnl += pnl
                    
                    # Check if we already recorded this resolution (avoid duplicates)
                    already_recorded = any(
                        rp.get("token_id") == token_id and 
                        rp.get("market_id") == market_id and
                        rp.get("resolved_at") == resolution_timestamp
                        for rp in tracker.resolved_positions
                    )
                    
                    if not already_recorded:
                        resolution_entry = {
                            "market_id": market_id,
                            "token_id": token_id,
                            "outcome": outcome,
                            "winning_outcome": winning_outcome,
                            "size": size,
                            "entry_price": entry_price,
                            "resolved_price": resolved_price,
                            "cost_basis": cost_basis,
                            "payout": payout,
                            "realized_pnl": pnl,
                            "resolved_at": resolution_timestamp,
                            "title": pos_info.get("title", ""),
                            "coin_name": pos_info.get("coin_name", "Unknown")
                        }
                        tracker.resolved_positions.append(resolution_entry)
                        tracker.realized_pnl += pnl
                        logger.info(f"✅ Applied resolution to closed position: {outcome} {'WON' if is_winner else 'LOST'} - "
                                  f"Size: {size:.2f}, Entry: ${entry_price:.4f}, Resolved: ${resolved_price:.4f}, "
                                  f"Cost: ${cost_basis:.2f}, Payout: ${payout:.2f}, PnL: ${pnl:.2f}")
                
                if positions_to_resolve:
                    total_resolution_pnl += market_resolution_pnl
                    resolution_count += 1
                    logger.info(f"✅ Market {market_id}: {len(open_positions_to_resolve)} open + {len(closed_positions_to_resolve)} closed = {len(positions_to_resolve)} positions resolved, total PnL: ${market_resolution_pnl:.2f}")
                    
                    # Log resolution details
                    resolution_log["resolutions_applied"].append({
                        "market_id": market_id,
                        "winning_outcome": winning_outcome,
                        "resolution_time": resolution_time.isoformat(),
                        "positions_count": len(positions_to_resolve),
                        "open_positions": len(open_positions_to_resolve),
                        "closed_positions": len(closed_positions_to_resolve),
                        "pnl": market_resolution_pnl
                    })
            
            logger.info(f"✅ Applied {resolution_count} resolutions, total resolution PnL: ${total_resolution_pnl:.2f}")
            
            # Save resolution log to file
            try:
                with open(resolution_log_file, 'w') as f:
                    json.dump(resolution_log, f, indent=2)
                logger.info(f"📝 Resolution log saved to {resolution_log_file}")
            except Exception as e:
                logger.error(f"❌ Failed to save resolution log: {e}")
        
            # Update status before saving (which might take a moment)
            try:
                if update.message and status_msg:
                    await status_msg.edit_text(
                        f"💾 Saving data...\n"
                        f"📊 Processed: {processed_count} trades\n"
                        f"📈 Resolutions: {resolution_count}\n"
                        f"💰 Resolution PnL: ${total_resolution_pnl:.2f}\n\n"
                        f"⏳ Calculating statistics..."
                    )
            except:
                pass
        
            # Save user state
            logger.info("💾 Saving user tracker state...")
            try:
                self.save_user_tracker(chat_id)
                logger.info("✅ User tracker state saved")
            except Exception as e:
                logger.error(f"❌ ERROR saving user tracker: {type(e).__name__}: {e}")
            
            # Get updated stats for the 24-hour period (trade-based PnL only)
            logger.info("📊 Calculating 24-hour stats...")
            try:
                stats = tracker.get_hourly_stats(start_timestamp, end_timestamp)
                logger.info(f"✅ 24-hour stats: pnl={stats['pnl']:.2f} (trade-based only), "
                           f"volume={stats['volume']:.2f}, trades={stats['trade_count']}, "
                           f"peak_exposure={stats.get('peak_concurrent_exposure', 0.0):.2f}")
            except Exception as e:
                logger.error(f"❌ ERROR calculating stats: {type(e).__name__}: {e}")
                stats = {
                    'pnl': 0.0,
                    'volume': 0.0,
                    'trade_count': 0,
                    'peak_concurrent_exposure': 0.0
                }
            
            # Send final summary (trade-based PnL only)
            pnl = stats['pnl']
            pnl_emoji = "✅" if pnl >= 0 else "❌"
            pnl_sign = "+" if pnl >= 0 else ""
            total_pnl = tracker.total_realized_pnl()
            open_count = len(tracker.snapshot_open_positions())
            resolved_count = len(tracker.resolved_positions)
            
            try:
                if update.message and status_msg:
                    await status_msg.edit_text(
                        f"✅ *Backfill All Complete!*\n\n"
                        f"📅 *Period:* {day_start.strftime('%B %d, %Y %I:%M %p')} - {now.strftime('%B %d, %Y %I:%M %p')}\n\n"
                        f"📊 *Processing Results:*\n"
                        f"   • Trades Found: {len(trades)}\n"
                        f"   • Processed: {processed_count}\n"
                        f"   • Skipped: {skipped_count}\n"
                        f"   • Resolutions Applied: {resolution_count}\n\n"
                        f"{pnl_emoji} *24-Hour PnL:* {pnl_sign}${pnl:.2f}\n"
                        f"💵 *Volume:* ${stats['volume']:.2f}\n"
                        f"💎 *Peak Exposure:* ${stats.get('peak_concurrent_exposure', 0.0):.2f}\n"
                        f"📈 *Total Trades:* {stats['trade_count']}\n\n"
                        f"📊 *Current Stats:*\n"
                        f"   💰 Total PnL: ${total_pnl:+.2f}\n"
                        f"   📈 Open Positions: {open_count}\n"
                        f"   🎯 Resolved Markets: {resolved_count}\n\n"
                        f"✨ *Features Used:*\n"
                        f"   ✅ Trade processing\n"
                        f"   ✅ Market resolution detection\n"
                        f"   ✅ Comprehensive analytics",
                        parse_mode='Markdown'
                    )
            except Exception as e:
                logger.error(f"❌ ERROR sending final summary: {type(e).__name__}: {e}")
            
            logger.info("=" * 80)
            logger.info("✅ BACKFILL ALL COMMAND COMPLETED SUCCESSFULLY")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"❌ FATAL ERROR in backfill all command: {type(e).__name__}: {e}")
            logger.exception("Full traceback:")
            try:
                if update.message:
                    await update.message.reply_text(
                        f"❌ Error during backfill all: {str(e)}\n"
                        "Please check the logs for details."
                    )
            except Exception:
                pass
    
    async def _handle_live(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /live command - enable live trading."""
        if not update.effective_chat:
            return
        chat_id = update.effective_chat.id
        self.active_chats.add(chat_id)
        
        if self.is_live_trading:
            status_msg = "🟢 *Already LIVE*\n\nTrading is already enabled. Real orders are being placed."
        else:
            self.is_live_trading = True
            self._save_trading_state()
            status_msg = (
                "🟢 *LIVE TRADING ENABLED*\n\n"
                "⚠️ Real orders will now be placed!\n"
                "The bot will copy trades and execute them on Polymarket.\n\n"
                "Use /stop to switch back to draft mode."
            )
            logger.warning("🟢 LIVE TRADING ENABLED - Real orders will be placed!")
        
        reply_keyboard = self._create_reply_keyboard()
        if update.message:
            await update.message.reply_text(
                status_msg,
                parse_mode='Markdown',
                reply_markup=reply_keyboard
            )
    
    async def _handle_prices(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /prices command - show live prices for Up/Down markets."""
        if not update.effective_chat:
            return
        chat_id = update.effective_chat.id
        
        # Get user's open positions grouped by market
        tracker = self.get_user_tracker(chat_id)
        open_positions = tracker.snapshot_open_positions()
        
        # Group positions by market_id
        markets = {}  # market_id -> {coin, title, tokens: {outcome: token_id}}
        
        for pos in open_positions:
            market_id = pos.get("market_id", "")
            if not market_id:
                continue
            
            if market_id not in markets:
                markets[market_id] = {
                    "coin": pos.get("coin_name", "Unknown"),
                    "title": pos.get("title", "Unknown Market"),
                    "tokens": {}
                }
            
            outcome = pos.get("outcome", "")
            token_id = pos.get("token_id", "")
            if outcome and token_id:
                markets[market_id]["tokens"][outcome] = token_id
        
        if not markets:
            if update.message:
                await update.message.reply_text(
                    "📊 No open positions found.\n"
                    "Prices will be shown when you have open positions."
                )
            return
        
        # Fetch all prices from CLOB API
        all_prices_response = None
        try:
            endpoint = f"{CLOB_API_BASE_URL}/prices"
            session = get_http_session()
            response = session.get(endpoint, timeout=10)
            if response.status_code == 200:
                all_prices_response = response.json()
        except Exception as e:
            logger.error(f"Error fetching all prices: {e}")
        
        # For each market, get both Up and Down tokens and prices
        lines = ["💹 Live Prices:\n"]
        found_any = False
        
        for market_id, market_data in markets.items():
            coin = market_data["coin"]
            title = market_data["title"]
            
            # Get both Up and Down token IDs for this market
            market_tokens = get_market_tokens(market_id)
            
            # If we didn't find tokens by market_id, try finding by token_id from position
            if not market_tokens and market_data["tokens"]:
                position_token_id = list(market_data["tokens"].values())[0]
                logger.info(f"Trying to find market by token_id {position_token_id[:20]}...")
                market_tokens = get_market_tokens_by_token_id(position_token_id)
                if market_tokens:
                    logger.info(f"Found {len(market_tokens)} tokens via token_id lookup: {list(market_tokens.keys())}")
            
            # Use tokens from API first (most reliable), fallback to position tokens
            up_token = market_tokens.get("UP") or market_data["tokens"].get("Up")
            down_token = market_tokens.get("DOWN") or market_data["tokens"].get("Down")
            
            if not up_token and not down_token:
                # Last resort: use the token we have from position (only one side)
                if market_data["tokens"]:
                    token_id = list(market_data["tokens"].values())[0]
                    outcome = list(market_data["tokens"].keys())[0]
                    if outcome.upper() == "UP":
                        up_token = token_id
                    else:
                        down_token = token_id
                else:
                    logger.warning(f"Could not find tokens for market {market_id}")
                    continue
            
            # Fetch prices for both tokens
            up_buy = up_sell = down_buy = down_sell = 0.0
            
            if all_prices_response:
                if up_token and up_token in all_prices_response:
                    up_prices = all_prices_response[up_token]
                    up_buy = float(up_prices.get("BUY", 0.0))
                    up_sell = float(up_prices.get("SELL", 0.0))
                
                if down_token and down_token in all_prices_response:
                    down_prices = all_prices_response[down_token]
                    down_buy = float(down_prices.get("BUY", 0.0))
                    down_sell = float(down_prices.get("SELL", 0.0))
            else:
                # Fallback: individual fetches
                if up_token:
                    up_buy = get_market_price(up_token, "BUY") or 0.0
                    up_sell = get_market_price(up_token, "SELL") or 0.0
                if down_token:
                    down_buy = get_market_price(down_token, "BUY") or 0.0
                    down_sell = get_market_price(down_token, "SELL") or 0.0
            
            # Use buy prices (best available) for display - only show prices fetched from API
            # Don't calculate missing side - spreads mean totals can be 101-105¢, not exactly 100¢
            up_price = up_buy if up_buy > 0 else up_sell
            down_price = down_buy if down_buy > 0 else down_sell
            
            # Only show if we have at least one price for either side
            if (up_price > 0) or (down_price > 0):
                found_any = True
                up_cents = int(up_price * 100) if up_price > 0 else 0
                down_cents = int(down_price * 100) if down_price > 0 else 0
                total_cents = up_cents + down_cents
                
                lines.append(f"📊 *{coin}*")
                lines.append(f"   {title}")
                
                # Show Up price (only if we fetched it from API)
                if up_price > 0:
                    lines.append(f"   🟢 Up: {up_cents}¢ (${up_price:.4f})")
                else:
                    lines.append(f"   🟢 Up: _No active orderbook_")
                
                # Show Down price (only if we fetched it from API)
                if down_price > 0:
                    lines.append(f"   🔴 Down: {down_cents}¢ (${down_price:.4f})")
                else:
                    lines.append(f"   🔴 Down: _No active orderbook_")
                
                # Show total only if we have both prices (may be 101-105¢ due to spreads)
                if up_price > 0 and down_price > 0:
                    lines.append(f"   Total: {total_cents}¢")
                
                lines.append("")
        
        if not found_any:
            if update.message:
                await update.message.reply_text(
                    "❌ No active prices found for your positions.\n\n"
                    "Markets may be closed or have no active orderbook."
                )
            return
        
        if update.message:
            await update.message.reply_text("\n".join(lines))
    
    async def _handle_stop(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /stop command - disable live trading (draft mode)."""
        if not update.effective_chat:
            return
        chat_id = update.effective_chat.id
        self.active_chats.add(chat_id)
        
        if not self.is_live_trading:
            status_msg = "🔴 *Already STOPPED*\n\nTrading is already in draft mode. No orders are being placed."
        else:
            self.is_live_trading = False
            self._save_trading_state()
            status_msg = (
                "🔴 *TRADING STOPPED*\n\n"
                "✅ Bot is now in DRAFT MODE.\n"
                "No real orders will be placed.\n"
                "Trades will still be tracked for PnL calculation.\n\n"
                "Use /live to enable live trading again."
            )
            logger.info("🔴 TRADING STOPPED - Bot is now in draft mode")
        
        reply_keyboard = self._create_reply_keyboard()
        if update.message:
            await update.message.reply_text(
                status_msg,
                parse_mode='Markdown',
                reply_markup=reply_keyboard
            )
    
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
    
    async def _show_pnl_hour_message(self, update: Update, tracker: PositionTracker, reply_keyboard: ReplyKeyboardMarkup) -> None:
        """Show PnL for current hour with reply keyboard."""
        if not update.message:
            return
        now = datetime.now()
        hour_start = now.replace(minute=0, second=0, microsecond=0)
        hour_end = hour_start + timedelta(hours=1)
        start_timestamp = int(hour_start.timestamp())
        end_timestamp = int(hour_end.timestamp())
        stats = tracker.get_hourly_stats(start_timestamp, end_timestamp)
        hourly_pnl = stats['pnl']
        total_pnl = tracker.total_realized_pnl()
        open_positions = tracker.snapshot_open_positions()
        closed_count = len(tracker.snapshot_closed_positions(limit=0))
        closed_this_hour = [
            p for p in tracker.snapshot_closed_positions(limit=0)
            if start_timestamp <= p.get('closed_at', 0) < end_timestamp
        ]
        pnl_emoji = "✅" if hourly_pnl >= 0 else "❌"
        pnl_sign = "+" if hourly_pnl >= 0 else ""
        total_emoji = "✅" if total_pnl >= 0 else "❌"
        total_sign = "+" if total_pnl >= 0 else ""
        hour_str = hour_start.strftime("%I:%M %p")
        hour_end_str = hour_end.strftime("%I:%M %p")
        message = (
            f"💰 Profit & Loss - Current Hour\n"
            f"⏰ {hour_str} - {hour_end_str}\n\n"
            f"{pnl_emoji} Hourly PnL: {pnl_sign}${hourly_pnl:.2f}\n"
            f"📊 Trades This Hour: {stats['trade_count']}\n"
            f"📉 Closed Positions: {len(closed_this_hour)}\n"
            f"💰 Volume: ${stats['volume']:.2f}\n\n"
            f"{total_emoji} All-Time PnL: {total_sign}${total_pnl:.2f}\n"
            f"📈 Open Positions: {len(open_positions)}\n"
            f"📉 Total Closed Trades: {closed_count}"
        )
        await update.message.reply_text(message, reply_markup=reply_keyboard)
    
    async def _show_pnl_today_message(self, update: Update, tracker: PositionTracker, reply_keyboard: ReplyKeyboardMarkup) -> None:
        """Show PnL for today with reply keyboard."""
        if not update.message:
            return
        now = datetime.now()
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        start_timestamp = int(today_start.timestamp())
        daily_pnl = tracker.realized_pnl_since(start_timestamp)
        total_pnl = tracker.total_realized_pnl()
        open_positions = tracker.snapshot_open_positions()
        closed_count = len(tracker.snapshot_closed_positions(limit=0))
        closed_today = [
            p for p in tracker.snapshot_closed_positions(limit=0)
            if p.get("closed_at", 0) >= start_timestamp
        ]
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
        await update.message.reply_text(message, reply_markup=reply_keyboard)
    
    async def _show_pnl_week_message(self, update: Update, tracker: PositionTracker, reply_keyboard: ReplyKeyboardMarkup) -> None:
        """Show PnL for this week with reply keyboard."""
        if not update.message:
            return
        now = datetime.now()
        week_start = now - timedelta(days=7)
        start_timestamp = int(week_start.timestamp())
        weekly_pnl = tracker.realized_pnl_since(start_timestamp)
        total_pnl = tracker.total_realized_pnl()
        open_positions = tracker.snapshot_open_positions()
        closed_count = len(tracker.snapshot_closed_positions(limit=0))
        pnl_emoji = "✅" if weekly_pnl >= 0 else "❌"
        pnl_sign = "+" if weekly_pnl >= 0 else ""
        total_emoji = "✅" if total_pnl >= 0 else "❌"
        total_sign = "+" if total_pnl >= 0 else ""
        message = (
            f"💰 Profit & Loss - This Week\n"
            f"📅 Last 7 Days\n\n"
            f"{pnl_emoji} Weekly PnL: {pnl_sign}${weekly_pnl:.2f}\n\n"
            f"{total_emoji} All-Time PnL: {total_sign}${total_pnl:.2f}\n"
            f"📈 Open Positions: {len(open_positions)}\n"
            f"📉 Total Closed Trades: {closed_count}"
        )
        await update.message.reply_text(message, reply_markup=reply_keyboard)
    
    async def _show_pnl_month_message(self, update: Update, tracker: PositionTracker, reply_keyboard: ReplyKeyboardMarkup) -> None:
        """Show PnL for this month with reply keyboard."""
        if not update.message:
            return
        now = datetime.now()
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        start_timestamp = int(month_start.timestamp())
        monthly_pnl = tracker.realized_pnl_since(start_timestamp)
        total_pnl = tracker.total_realized_pnl()
        open_positions = tracker.snapshot_open_positions()
        closed_count = len(tracker.snapshot_closed_positions(limit=0))
        pnl_emoji = "✅" if monthly_pnl >= 0 else "❌"
        pnl_sign = "+" if monthly_pnl >= 0 else ""
        total_emoji = "✅" if total_pnl >= 0 else "❌"
        total_sign = "+" if total_pnl >= 0 else ""
        message = (
            f"💰 Profit & Loss - This Month\n"
            f"📅 Since {month_start.strftime('%B 1, %Y')}\n\n"
            f"{pnl_emoji} Monthly PnL: {pnl_sign}${monthly_pnl:.2f}\n\n"
            f"{total_emoji} All-Time PnL: {total_sign}${total_pnl:.2f}\n"
            f"📈 Open Positions: {len(open_positions)}\n"
            f"📉 Total Closed Trades: {closed_count}"
        )
        await update.message.reply_text(message, reply_markup=reply_keyboard)
    
    async def _show_pnl_alltime_message(self, update: Update, tracker: PositionTracker, reply_keyboard: ReplyKeyboardMarkup) -> None:
        """Show all-time PnL with reply keyboard."""
        if not update.message:
            return
        total_pnl = tracker.total_realized_pnl()
        open_positions = tracker.snapshot_open_positions()
        closed_count = len(tracker.snapshot_closed_positions(limit=0))
        pnl_emoji = "✅" if total_pnl >= 0 else "❌"
        pnl_sign = "+" if total_pnl >= 0 else ""
        message = (
            f"💰 Profit & Loss - All Time\n\n"
            f"{pnl_emoji} Total PnL: {pnl_sign}${total_pnl:.2f}\n"
            f"📈 Open Positions: {len(open_positions)}\n"
            f"📉 Total Closed Trades: {closed_count}"
        )
        await update.message.reply_text(message, reply_markup=reply_keyboard)
    
    async def _show_open_positions_message(self, update: Update, tracker: PositionTracker, reply_keyboard: ReplyKeyboardMarkup) -> None:
        """Show open positions with reply keyboard."""
        if not update.message:
            return
        positions = tracker.snapshot_open_positions()
        if not positions:
            await update.message.reply_text("📭 No open positions.", reply_markup=reply_keyboard)
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
        await update.message.reply_text("\n".join(lines), reply_markup=reply_keyboard)
    
    async def _show_closed_trades_message(self, update: Update, tracker: PositionTracker, reply_keyboard: ReplyKeyboardMarkup) -> None:
        """Show closed trades with reply keyboard."""
        if not update.message:
            return
        closed = tracker.snapshot_closed_positions(limit=10)
        if not closed:
            await update.message.reply_text("📭 No closed trades yet.", reply_markup=reply_keyboard)
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
        await update.message.reply_text("\n".join(lines), reply_markup=reply_keyboard)
    
    async def _show_money_stats_message(self, update: Update, tracker: PositionTracker, reply_keyboard: ReplyKeyboardMarkup) -> None:
        """Show money stats with reply keyboard."""
        if not update.message:
            return
        now = datetime.now()
        hour_start = now.replace(minute=0, second=0, microsecond=0)
        hour_end = hour_start + timedelta(hours=1)
        start_timestamp = int(hour_start.timestamp())
        end_timestamp = int(hour_end.timestamp())
        stats = tracker.get_hourly_stats(start_timestamp, end_timestamp)
        hour_str = hour_start.strftime("%I:%M %p")
        message = (
            f"💵 Money Stats - Current Hour\n"
            f"⏰ {hour_str}\n\n"
            f"💰 Volume: ${stats['volume']:.2f}\n"
            f"💎 Peak Concurrent Exposure: ${stats.get('peak_concurrent_exposure', 0.0):.2f}\n"
            f"📊 Trades: {stats['trade_count']}"
        )
        if stats.get('max_trade'):
            max_t = stats['max_trade']
            coin = max_t.get("coin_name", "Unknown")
            outcome = max_t.get("outcome", "Unknown")
            message += (
                f"\n\n🔥 Largest Trade:\n"
                f"   {coin} {outcome} - ${stats.get('max_value', 0.0):.2f}"
            )
        await update.message.reply_text(message, reply_markup=reply_keyboard)
    
    async def _show_main_menu_message(self, update: Update, tracker: PositionTracker, reply_keyboard: ReplyKeyboardMarkup) -> None:
        """Show main menu with reply keyboard."""
        if not update.message:
            return
        total_pnl = tracker.total_realized_pnl()
        menu_text = (
            "📊 *Main Menu*\n\n"
            f"💰 Total PnL: ${total_pnl:+.2f}\n\n"
            "Select an option below:"
        )
        await update.message.reply_text(
            menu_text,
            parse_mode='Markdown',
            reply_markup=reply_keyboard
        )
    
    async def _handle_keyboard_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle messages from the reply keyboard buttons."""
        if not update.message or not update.message.text or not update.effective_chat:
            return
        
        chat_id = update.effective_chat.id
        self.active_chats.add(chat_id)
        tracker = self.get_user_tracker(chat_id)
        text = update.message.text.strip()
        
        reply_keyboard = self._create_reply_keyboard()
        
        # Handle keyboard button presses
        if text == "📊 PnL Hour":
            await self._show_pnl_hour_message(update, tracker, reply_keyboard)
        elif text == "📅 PnL Today":
            await self._show_pnl_today_message(update, tracker, reply_keyboard)
        elif text == "📆 PnL Week":
            await self._show_pnl_week_message(update, tracker, reply_keyboard)
        elif text == "📆 PnL Month":
            await self._show_pnl_month_message(update, tracker, reply_keyboard)
        elif text == "💰 PnL All Time":
            await self._show_pnl_alltime_message(update, tracker, reply_keyboard)
        elif text == "🔄 Backfill":
            await update.message.reply_text(
                "🔄 *Backfill*\n\n"
                "Please use the /backfill command for full backfill functionality.\n"
                "This will reset and refresh the current hour's data.",
                parse_mode='Markdown',
                reply_markup=reply_keyboard
            )
        elif text == "🔄 Backfill All (24h)":
            # Trigger the backfill all command
            await self._handle_backfill_all(update, context)
        elif text == "📈 Open Positions":
            await self._show_open_positions_message(update, tracker, reply_keyboard)
        elif text == "📉 Closed Trades":
            await self._show_closed_trades_message(update, tracker, reply_keyboard)
        elif text == "💵 Money Stats":
            await self._show_money_stats_message(update, tracker, reply_keyboard)
        elif text == "🏠 Menu":
            await self._show_main_menu_message(update, tracker, reply_keyboard)
        elif text == "🟢 LIVE" or text == "🔴 STOP":
            # Toggle trading state
            if self.is_live_trading:
                await self._handle_stop(update, context)
            else:
                await self._handle_live(update, context)
        else:
            # Unknown message, show menu
            await self._show_main_menu_message(update, tracker, reply_keyboard)
    
    async def _handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle callback queries from inline keyboard buttons."""
        query = update.callback_query
        if not query or not query.data:
            return
        
        await query.answer()  # Acknowledge the callback
        
        chat_id = query.message.chat.id if query.message else None
        if not chat_id:
            return
        
        self.active_chats.add(chat_id)
        tracker = self.get_user_tracker(chat_id)
        
        callback_data = query.data
        
        if callback_data == "main_menu":
            await self._show_main_menu(query)
        elif callback_data == "pnl_hour":
            await self._show_pnl_hour(query, tracker)
        elif callback_data == "pnl_today":
            await self._show_pnl_today(query, tracker)
        elif callback_data == "pnl_week":
            await self._show_pnl_week(query, tracker)
        elif callback_data == "pnl_month":
            await self._show_pnl_month(query, tracker)
        elif callback_data == "pnl_alltime":
            await self._show_pnl_alltime(query, tracker)
        elif callback_data == "backfill":
            await self._handle_backfill_callback(query, tracker)
        elif callback_data == "open_positions":
            await self._show_open_positions(query, tracker)
        elif callback_data == "closed_trades":
            await self._show_closed_trades(query, tracker)
        elif callback_data == "live_prices":
            if not query.message or not query.message.chat:
                await query.answer("Error: Invalid query")
                return
            chat_id = query.message.chat.id
            tracker = self.get_user_tracker(chat_id)
            open_positions = tracker.snapshot_open_positions()
            
            # Group positions by market_id
            markets = {}
            
            for pos in open_positions:
                market_id = pos.get("market_id", "")
                if not market_id:
                    continue
                
                if market_id not in markets:
                    markets[market_id] = {
                        "coin": pos.get("coin_name", "Unknown"),
                        "title": pos.get("title", "Unknown Market"),
                        "tokens": {}
                    }
                
                outcome = pos.get("outcome", "")
                token_id = pos.get("token_id", "")
                if outcome and token_id:
                    markets[market_id]["tokens"][outcome] = token_id
            
            if not markets:
                await query.answer("No open positions found.")
                await query.edit_message_text(
                    "📊 No open positions found.\n"
                    "Prices will be shown when you have open positions."
                )
                return
            
            # Fetch all prices
            all_prices_response = None
            try:
                endpoint = f"{CLOB_API_BASE_URL}/prices"
                session = get_http_session()
                response = session.get(endpoint, timeout=10)
                if response.status_code == 200:
                    all_prices_response = response.json()
            except Exception as e:
                logger.error(f"Error fetching all prices: {e}")
            
            lines = ["💹 *Live Prices:*\n"]
            found_any = False
            
            for market_id, market_data in markets.items():
                coin = market_data["coin"]
                title = market_data["title"]
                
                market_tokens = get_market_tokens(market_id)
                
                # If we didn't find tokens by market_id, try finding by token_id from position
                if not market_tokens and market_data["tokens"]:
                    position_token_id = list(market_data["tokens"].values())[0]
                    market_tokens = get_market_tokens_by_token_id(position_token_id)
                
                up_token = market_tokens.get("UP") or market_data["tokens"].get("Up")
                down_token = market_tokens.get("DOWN") or market_data["tokens"].get("Down")
                
                if not up_token and not down_token:
                    if market_data["tokens"]:
                        token_id = list(market_data["tokens"].values())[0]
                        outcome = list(market_data["tokens"].keys())[0]
                        if outcome.upper() == "UP":
                            up_token = token_id
                        else:
                            down_token = token_id
                    else:
                        logger.warning(f"Could not find tokens for market {market_id}")
                        continue
                
                up_buy = up_sell = down_buy = down_sell = 0.0
                
                if all_prices_response:
                    if up_token and up_token in all_prices_response:
                        up_prices = all_prices_response[up_token]
                        up_buy = float(up_prices.get("BUY", 0.0))
                        up_sell = float(up_prices.get("SELL", 0.0))
                    if down_token and down_token in all_prices_response:
                        down_prices = all_prices_response[down_token]
                        down_buy = float(down_prices.get("BUY", 0.0))
                        down_sell = float(down_prices.get("SELL", 0.0))
                else:
                    if up_token:
                        up_buy = get_market_price(up_token, "BUY") or 0.0
                        up_sell = get_market_price(up_token, "SELL") or 0.0
                    if down_token:
                        down_buy = get_market_price(down_token, "BUY") or 0.0
                        down_sell = get_market_price(down_token, "SELL") or 0.0
                
                up_price = up_buy if up_buy > 0 else up_sell
                down_price = down_buy if down_buy > 0 else down_sell
                
                # Only show prices fetched from API - don't calculate (spreads mean totals can be 101-105¢)
                if (up_price > 0) or (down_price > 0):
                    found_any = True
                    up_cents = int(up_price * 100) if up_price > 0 else 0
                    down_cents = int(down_price * 100) if down_price > 0 else 0
                    total_cents = up_cents + down_cents
                    
                    lines.append(f"📊 *{coin}*")
                    lines.append(f"   {title}")
                    
                    # Show Up price (only if fetched from API)
                    if up_price > 0:
                        lines.append(f"   🟢 Up: {up_cents}¢ (${up_price:.4f})")
                    else:
                        lines.append(f"   🟢 Up: _No active orderbook_")
                    
                    # Show Down price (only if fetched from API)
                    if down_price > 0:
                        lines.append(f"   🔴 Down: {down_cents}¢ (${down_price:.4f})")
                    else:
                        lines.append(f"   🔴 Down: _No active orderbook_")
                    
                    # Show total only if we have both prices (may be 101-105¢ due to spreads)
                    if up_price > 0 and down_price > 0:
                        lines.append(f"   Total: {total_cents}¢")
                    
                    lines.append("")
            
            if not found_any:
                await query.answer("No prices available")
                await query.edit_message_text(
                    "❌ No active prices found.\n\n"
                    "Markets may be closed or have no active orderbook."
                )
                return
            
            await query.answer("Prices updated!")
            await query.edit_message_text("\n".join(lines), parse_mode='Markdown')
        elif callback_data == "money_stats":
            await self._show_money_stats(query, tracker)
        elif callback_data == "reset":
            await self._handle_reset_callback(query, tracker)
    
    async def _show_main_menu(self, query) -> None:
        """Show the main menu."""
        tracker = self.get_user_tracker(query.message.chat.id)
        total_pnl = tracker.total_realized_pnl()
        
        menu_text = (
            "📊 *Main Menu*\n\n"
            f"💰 Total PnL: ${total_pnl:+.2f}\n\n"
            "Select an option below:"
        )
        
        keyboard = self._create_menu_keyboard()
        await query.edit_message_text(
            menu_text,
            parse_mode='Markdown',
            reply_markup=keyboard
        )
    
    async def _show_pnl_hour(self, query, tracker: PositionTracker) -> None:
        """Show PnL for current hour with trade details."""
        now = datetime.now()
        hour_start = now.replace(minute=0, second=0, microsecond=0)
        hour_end = hour_start + timedelta(hours=1)
        start_timestamp = int(hour_start.timestamp())
        end_timestamp = int(hour_end.timestamp())
        
        stats = tracker.get_hourly_stats(start_timestamp, end_timestamp)
        hourly_pnl = stats['pnl']
        peak_exposure = stats.get('peak_concurrent_exposure', 0.0)
        
        # Get trades in this hour
        period_trades = [
            t for t in tracker.trade_history
            if start_timestamp <= t.get("timestamp", 0) < end_timestamp
        ]
        period_trades.sort(key=lambda t: t.get("timestamp", 0))
        
        # Get closed positions this hour
        closed_this_hour = []
        for entry in tracker.snapshot_closed_positions(limit=0):
            closed_at = entry.get("closed_at", 0)
            if isinstance(closed_at, float):
                closed_at = int(closed_at)
            if start_timestamp <= closed_at < end_timestamp:
                closed_this_hour.append(entry)
        
        # Calculate live exposure
        open_positions = tracker.snapshot_open_positions()
        live_exposure = sum(
            abs(float(pos.get("net_size", 0.0))) * float(pos.get("avg_entry_price", 0.0))
            for pos in open_positions
        )
        
        pnl_emoji = "✅" if hourly_pnl >= 0 else "❌"
        pnl_sign = "+" if hourly_pnl >= 0 else ""
        
        message = (
            f"💰 *Profit & Loss - Current Hour*\n"
            f"⏰ {hour_start.strftime('%I:%M %p')} - {hour_end.strftime('%I:%M %p')}\n\n"
            f"{pnl_emoji} *Hourly PnL:* {pnl_sign}${hourly_pnl:.2f}\n"
            f"📊 *Trades:* {len(period_trades)}\n"
            f"📉 *Closed Positions:* {len(closed_this_hour)}\n"
            f"💵 *Volume:* ${stats['volume']:.2f}\n\n"
            f"💎 *Peak Concurrent Exposure:* ${peak_exposure:.2f}\n"
            f"💎 *Live Concurrent Exposure:* ${live_exposure:.2f}\n\n"
            f"✨ *Features:* Rate limit protection • Trade tracking • Real-time updates"
        )
        
        # Add trade details (last 5 trades)
        if period_trades:
            message += "\n📋 *Recent Trades:*\n"
            for trade in period_trades[-5:]:
                timestamp = trade.get("timestamp", 0)
                trade_time = datetime.fromtimestamp(timestamp).strftime("%I:%M %p")
                outcome = trade.get("outcome", "Unknown")
                side = trade.get("side", "")
                size = trade.get("my_size", 0.0)
                price = trade.get("price", 0.0)
                value = trade.get("my_usdc_value", 0.0)
                side_emoji = "🟢" if side == "BUY" else "🔴"
                message += (
                    f"   {side_emoji} {trade_time} | {outcome} {side} | "
                    f"{size:.2f} @ ${price:.4f} | ${value:.2f}\n"
                )
        
        keyboard = self._create_menu_keyboard()
        await query.edit_message_text(
            message,
            parse_mode='Markdown',
            reply_markup=keyboard
        )
    
    async def _show_pnl_today(self, query, tracker: PositionTracker) -> None:
        """Show PnL for today with trade details."""
        now = datetime.now()
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        start_timestamp = int(today_start.timestamp())
        end_timestamp = int(now.timestamp())
        
        stats = tracker.get_hourly_stats(start_timestamp, end_timestamp)
        daily_pnl = stats['pnl']
        peak_exposure = stats.get('peak_concurrent_exposure', 0.0)
        
        # Get trades today
        period_trades = [
            t for t in tracker.trade_history
            if start_timestamp <= t.get("timestamp", 0) < end_timestamp
        ]
        period_trades.sort(key=lambda t: t.get("timestamp", 0))
        
        # Get closed positions today
        closed_today = []
        for entry in tracker.snapshot_closed_positions(limit=0):
            closed_at = entry.get("closed_at", 0)
            if isinstance(closed_at, float):
                closed_at = int(closed_at)
            if closed_at >= start_timestamp:
                closed_today.append(entry)
        
        # Calculate live exposure
        open_positions = tracker.snapshot_open_positions()
        live_exposure = sum(
            abs(float(pos.get("net_size", 0.0))) * float(pos.get("avg_entry_price", 0.0))
            for pos in open_positions
        )
        
        pnl_emoji = "✅" if daily_pnl >= 0 else "❌"
        pnl_sign = "+" if daily_pnl >= 0 else ""
        
        message = (
            f"💰 *Profit & Loss - Today*\n"
            f"📅 {today_start.strftime('%B %d, %Y')} (since 12:00 AM)\n\n"
            f"{pnl_emoji} *Daily PnL:* {pnl_sign}${daily_pnl:.2f}\n"
            f"📊 *Trades:* {len(period_trades)}\n"
            f"📉 *Closed Positions:* {len(closed_today)}\n"
            f"💵 *Volume:* ${stats['volume']:.2f}\n\n"
            f"💎 *Peak Concurrent Exposure:* ${peak_exposure:.2f}\n"
            f"💎 *Live Concurrent Exposure:* ${live_exposure:.2f}\n"
        )
        
        # Add trade summary
        if period_trades:
            buy_count = sum(1 for t in period_trades if t.get("side") == "BUY")
            sell_count = sum(1 for t in period_trades if t.get("side") == "SELL")
            message += f"\n📋 *Trade Summary:*\n"
            message += f"   🟢 Buys: {buy_count} | 🔴 Sells: {sell_count}\n"
            
            # Show last 5 trades
            message += "\n📋 *Recent Trades:*\n"
            for trade in period_trades[-5:]:
                timestamp = trade.get("timestamp", 0)
                trade_time = datetime.fromtimestamp(timestamp).strftime("%I:%M %p")
                outcome = trade.get("outcome", "Unknown")
                side = trade.get("side", "")
                size = trade.get("my_size", 0.0)
                price = trade.get("price", 0.0)
                value = trade.get("my_usdc_value", 0.0)
                side_emoji = "🟢" if side == "BUY" else "🔴"
                message += (
                    f"   {side_emoji} {trade_time} | {outcome} {side} | "
                    f"{size:.2f} @ ${price:.4f} | ${value:.2f}\n"
                )
        
        keyboard = self._create_menu_keyboard()
        await query.edit_message_text(
            message,
            parse_mode='Markdown',
            reply_markup=keyboard
        )
    
    async def _show_pnl_week(self, query, tracker: PositionTracker) -> None:
        """Show PnL for this week with trade details."""
        now = datetime.now()
        # Get start of week (Monday)
        days_since_monday = now.weekday()
        week_start = (now - timedelta(days=days_since_monday)).replace(hour=0, minute=0, second=0, microsecond=0)
        start_timestamp = int(week_start.timestamp())
        end_timestamp = int(now.timestamp())
        
        stats = tracker.get_hourly_stats(start_timestamp, end_timestamp)
        weekly_pnl = stats['pnl']
        peak_exposure = stats.get('peak_concurrent_exposure', 0.0)
        
        # Get trades this week
        period_trades = [
            t for t in tracker.trade_history
            if start_timestamp <= t.get("timestamp", 0) < end_timestamp
        ]
        period_trades.sort(key=lambda t: t.get("timestamp", 0))
        
        # Get closed positions this week
        closed_this_week = []
        for entry in tracker.snapshot_closed_positions(limit=0):
            closed_at = entry.get("closed_at", 0)
            if isinstance(closed_at, float):
                closed_at = int(closed_at)
            if closed_at >= start_timestamp:
                closed_this_week.append(entry)
        
        # Calculate live exposure
        open_positions = tracker.snapshot_open_positions()
        live_exposure = sum(
            abs(float(pos.get("net_size", 0.0))) * float(pos.get("avg_entry_price", 0.0))
            for pos in open_positions
        )
        
        pnl_emoji = "✅" if weekly_pnl >= 0 else "❌"
        pnl_sign = "+" if weekly_pnl >= 0 else ""
        
        message = (
            f"💰 *Profit & Loss - This Week*\n"
            f"📅 {week_start.strftime('%B %d')} - {now.strftime('%B %d, %Y')}\n\n"
            f"{pnl_emoji} *Weekly PnL:* {pnl_sign}${weekly_pnl:.2f}\n"
            f"📊 *Trades:* {len(period_trades)}\n"
            f"📉 *Closed Positions:* {len(closed_this_week)}\n"
            f"💵 *Volume:* ${stats['volume']:.2f}\n\n"
            f"💎 *Peak Concurrent Exposure:* ${peak_exposure:.2f}\n"
            f"💎 *Live Concurrent Exposure:* ${live_exposure:.2f}\n"
        )
        
        # Add trade summary
        if period_trades:
            buy_count = sum(1 for t in period_trades if t.get("side") == "BUY")
            sell_count = sum(1 for t in period_trades if t.get("side") == "SELL")
            message += f"\n📋 *Trade Summary:*\n"
            message += f"   🟢 Buys: {buy_count} | 🔴 Sells: {sell_count}\n"
            
            # Show last 5 trades
            message += "\n📋 *Recent Trades:*\n"
            for trade in period_trades[-5:]:
                timestamp = trade.get("timestamp", 0)
                trade_time = datetime.fromtimestamp(timestamp).strftime("%b %d %I:%M %p")
                outcome = trade.get("outcome", "Unknown")
                side = trade.get("side", "")
                size = trade.get("my_size", 0.0)
                price = trade.get("price", 0.0)
                value = trade.get("my_usdc_value", 0.0)
                side_emoji = "🟢" if side == "BUY" else "🔴"
                message += (
                    f"   {side_emoji} {trade_time} | {outcome} {side} | "
                    f"{size:.2f} @ ${price:.4f} | ${value:.2f}\n"
                )
        
        keyboard = self._create_menu_keyboard()
        await query.edit_message_text(
            message,
            parse_mode='Markdown',
            reply_markup=keyboard
        )
    
    async def _show_pnl_month(self, query, tracker: PositionTracker) -> None:
        """Show PnL for this month with trade details."""
        now = datetime.now()
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        start_timestamp = int(month_start.timestamp())
        end_timestamp = int(now.timestamp())
        
        stats = tracker.get_hourly_stats(start_timestamp, end_timestamp)
        monthly_pnl = stats['pnl']
        peak_exposure = stats.get('peak_concurrent_exposure', 0.0)
        
        # Get trades this month
        period_trades = [
            t for t in tracker.trade_history
            if start_timestamp <= t.get("timestamp", 0) < end_timestamp
        ]
        period_trades.sort(key=lambda t: t.get("timestamp", 0))
        
        # Get closed positions this month
        closed_this_month = []
        for entry in tracker.snapshot_closed_positions(limit=0):
            closed_at = entry.get("closed_at", 0)
            if isinstance(closed_at, float):
                closed_at = int(closed_at)
            if closed_at >= start_timestamp:
                closed_this_month.append(entry)
        
        # Calculate live exposure
        open_positions = tracker.snapshot_open_positions()
        live_exposure = sum(
            abs(float(pos.get("net_size", 0.0))) * float(pos.get("avg_entry_price", 0.0))
            for pos in open_positions
        )
        
        pnl_emoji = "✅" if monthly_pnl >= 0 else "❌"
        pnl_sign = "+" if monthly_pnl >= 0 else ""
        
        message = (
            f"💰 *Profit & Loss - This Month*\n"
            f"📅 {month_start.strftime('%B %Y')}\n\n"
            f"{pnl_emoji} *Monthly PnL:* {pnl_sign}${monthly_pnl:.2f}\n"
            f"📊 *Trades:* {len(period_trades)}\n"
            f"📉 *Closed Positions:* {len(closed_this_month)}\n"
            f"💵 *Volume:* ${stats['volume']:.2f}\n\n"
            f"💎 *Peak Concurrent Exposure:* ${peak_exposure:.2f}\n"
            f"💎 *Live Concurrent Exposure:* ${live_exposure:.2f}\n"
        )
        
        # Add trade summary
        if period_trades:
            buy_count = sum(1 for t in period_trades if t.get("side") == "BUY")
            sell_count = sum(1 for t in period_trades if t.get("side") == "SELL")
            message += f"\n📋 *Trade Summary:*\n"
            message += f"   🟢 Buys: {buy_count} | 🔴 Sells: {sell_count}\n"
            
            # Show last 5 trades
            message += "\n📋 *Recent Trades:*\n"
            for trade in period_trades[-5:]:
                timestamp = trade.get("timestamp", 0)
                trade_time = datetime.fromtimestamp(timestamp).strftime("%b %d %I:%M %p")
                outcome = trade.get("outcome", "Unknown")
                side = trade.get("side", "")
                size = trade.get("my_size", 0.0)
                price = trade.get("price", 0.0)
                value = trade.get("my_usdc_value", 0.0)
                side_emoji = "🟢" if side == "BUY" else "🔴"
                message += (
                    f"   {side_emoji} {trade_time} | {outcome} {side} | "
                    f"{size:.2f} @ ${price:.4f} | ${value:.2f}\n"
                )
        
        keyboard = self._create_menu_keyboard()
        await query.edit_message_text(
            message,
            parse_mode='Markdown',
            reply_markup=keyboard
        )
    
    async def _show_pnl_alltime(self, query, tracker: PositionTracker) -> None:
        """Show PnL for all time with comprehensive trade details."""
        total_pnl = tracker.total_realized_pnl()
        all_trades = tracker.trade_history
        all_trades.sort(key=lambda t: t.get("timestamp", 0))
        
        # Get all closed positions
        closed_all = tracker.snapshot_closed_positions(limit=0)
        
        # Calculate peak exposure from all time
        if all_trades:
            first_timestamp = all_trades[0].get("timestamp", 0)
            last_timestamp = all_trades[-1].get("timestamp", 0) if all_trades else int(datetime.now().timestamp())
            stats = tracker.get_hourly_stats(first_timestamp, last_timestamp + 1)
            peak_exposure = stats.get('peak_concurrent_exposure', 0.0)
            total_volume = stats.get('volume', 0.0)
        else:
            peak_exposure = 0.0
            total_volume = 0.0
        
        # Calculate live exposure
        open_positions = tracker.snapshot_open_positions()
        live_exposure = sum(
            abs(float(pos.get("net_size", 0.0))) * float(pos.get("avg_entry_price", 0.0))
            for pos in open_positions
        )
        
        pnl_emoji = "✅" if total_pnl >= 0 else "❌"
        pnl_sign = "+" if total_pnl >= 0 else ""
        
        message = (
            f"💰 *Profit & Loss - All Time*\n\n"
            f"{pnl_emoji} *Total PnL:* {pnl_sign}${total_pnl:.2f}\n"
            f"📊 *Total Trades:* {len(all_trades)}\n"
            f"📉 *Total Closed Positions:* {len(closed_all)}\n"
            f"💵 *Total Volume:* ${total_volume:.2f}\n\n"
            f"💎 *Peak Concurrent Exposure:* ${peak_exposure:.2f}\n"
            f"💎 *Live Concurrent Exposure:* ${live_exposure:.2f}\n"
        )
        
        # Add comprehensive trade summary
        if all_trades:
            buy_count = sum(1 for t in all_trades if t.get("side") == "BUY")
            sell_count = sum(1 for t in all_trades if t.get("side") == "SELL")
            total_buy_value = sum(t.get("my_usdc_value", 0.0) for t in all_trades if t.get("side") == "BUY")
            total_sell_value = sum(t.get("my_usdc_value", 0.0) for t in all_trades if t.get("side") == "SELL")
            
            message += f"\n📋 *Trade Summary:*\n"
            message += f"   🟢 Buys: {buy_count} (${total_buy_value:.2f})\n"
            message += f"   🔴 Sells: {sell_count} (${total_sell_value:.2f})\n"
            
            # Show last 5 trades
            message += "\n📋 *Recent Trades:*\n"
            for trade in all_trades[-5:]:
                timestamp = trade.get("timestamp", 0)
                trade_time = datetime.fromtimestamp(timestamp).strftime("%b %d %I:%M %p")
                outcome = trade.get("outcome", "Unknown")
                side = trade.get("side", "")
                size = trade.get("my_size", 0.0)
                price = trade.get("price", 0.0)
                value = trade.get("my_usdc_value", 0.0)
                side_emoji = "🟢" if side == "BUY" else "🔴"
                message += (
                    f"   {side_emoji} {trade_time} | {outcome} {side} | "
                    f"{size:.2f} @ ${price:.4f} | ${value:.2f}\n"
                )
        
        keyboard = self._create_menu_keyboard()
        await query.edit_message_text(
            message,
            parse_mode='Markdown',
            reply_markup=keyboard
        )
    
    async def _handle_backfill_callback(self, query, tracker: PositionTracker) -> None:
        """Handle backfill from callback."""
        # This is a simplified version - full backfill should be done via command
        await query.edit_message_text(
            "🔄 *Backfill*\n\n"
            "Please use the /backfill command for full backfill functionality.\n"
            "This will reset and refresh the current hour's data.",
            parse_mode='Markdown',
            reply_markup=self._create_menu_keyboard()
        )
    
    async def _show_open_positions(self, query, tracker: PositionTracker) -> None:
        """Show open positions."""
        positions = tracker.snapshot_open_positions()
        
        if not positions:
            message = "📈 *Open Positions*\n\nNo open positions."
        else:
            message = f"📈 *Open Positions* ({len(positions)})\n\n"
            for idx, pos in enumerate(positions[-10:], 1):  # Show last 10
                outcome = pos.get("outcome", "Unknown")
                net_size = pos.get("net_size", 0.0)
                avg_price = pos.get("avg_entry_price", 0.0)
                value = abs(net_size) * avg_price
                message += (
                    f"{idx}. {outcome}\n"
                    f"   Size: {net_size:.2f} @ ${avg_price:.4f}\n"
                    f"   Value: ${value:.2f}\n\n"
                )
            if len(positions) > 10:
                message += f"... and {len(positions) - 10} more positions"
        
        keyboard = self._create_menu_keyboard()
        await query.edit_message_text(
            message,
            parse_mode='Markdown',
            reply_markup=keyboard
        )
    
    async def _show_closed_trades(self, query, tracker: PositionTracker) -> None:
        """Show closed trades."""
        closed = tracker.snapshot_closed_positions(limit=10)
        
        if not closed:
            message = "📉 *Closed Trades*\n\nNo closed trades yet."
        else:
            message = f"📉 *Recent Closed Trades* (Last 10)\n\n"
            for idx, entry in enumerate(closed, 1):
                outcome = entry.get("outcome", "Unknown")
                pnl = entry.get("realized_pnl", 0.0)
                closed_at = entry.get("closed_at", 0)
                if isinstance(closed_at, float):
                    closed_at = int(closed_at)
                time_str = datetime.fromtimestamp(closed_at).strftime("%b %d %I:%M %p")
                pnl_emoji = "✅" if pnl >= 0 else "❌"
                pnl_sign = "+" if pnl >= 0 else ""
                message += (
                    f"{idx}. {outcome}\n"
                    f"   {pnl_emoji} PnL: {pnl_sign}${pnl:.2f}\n"
                    f"   📅 {time_str}\n\n"
                )
        
        keyboard = self._create_menu_keyboard()
        await query.edit_message_text(
            message,
            parse_mode='Markdown',
            reply_markup=keyboard
        )
    
    async def _show_money_stats(self, query, tracker: PositionTracker) -> None:
        """Show money/volume statistics."""
        now = datetime.now()
        hour_start = now.replace(minute=0, second=0, microsecond=0)
        hour_end = hour_start + timedelta(hours=1)
        start_timestamp = int(hour_start.timestamp())
        end_timestamp = int(hour_end.timestamp())
        
        stats = tracker.get_hourly_stats(start_timestamp, end_timestamp)
        
        message = (
            f"💵 *Money Statistics - Current Hour*\n"
            f"⏰ {hour_start.strftime('%I:%M %p')} - {hour_end.strftime('%I:%M %p')}\n\n"
            f"💵 *Volume:* ${stats['volume']:.2f}\n"
            f"📊 *Trades:* {stats['trade_count']}\n"
            f"💎 *Peak Concurrent Exposure:* ${stats.get('peak_concurrent_exposure', 0.0):.2f}\n"
        )
        
        if stats.get('max_trade'):
            max_trade = stats['max_trade']
            message += (
                f"\n📈 *Largest Trade:*\n"
                f"   {max_trade.get('outcome', 'Unknown')} {max_trade.get('side', '')}\n"
                f"   ${stats.get('max_value', 0.0):.2f}\n"
            )
        
        keyboard = self._create_menu_keyboard()
        await query.edit_message_text(
            message,
            parse_mode='Markdown',
            reply_markup=keyboard
        )
    
    async def _handle_reset_callback(self, query, tracker: PositionTracker) -> None:
        """Handle reset from callback."""
        await query.edit_message_text(
            "🔄 *Reset*\n\n"
            "Please use the /reset command to reset all positions and trades.\n"
            "This action cannot be undone!",
            parse_mode='Markdown',
            reply_markup=self._create_menu_keyboard()
        )


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


def parse_resolution_time_from_title(title: str) -> Optional[datetime]:
    """
    Parse resolution time from market title.
    Format: "Coin Up or Down - December 3, 6AM ET"
    Returns datetime object or None if parsing fails.
    """
    if not title:
        return None
    
    try:
        import re
        from dateutil import parser as date_parser
        
        # Pattern: "December 3, 6AM ET" or "December 3, 6:00 AM ET"
        # Extract the date/time part after the dash
        match = re.search(r'-\s*([^-]+)$', title)
        if not match:
            return None
        
        date_str = match.group(1).strip()
        # Parse the date string
        dt = date_parser.parse(date_str, fuzzy=True)
        return dt
    except Exception as e:
        logger.debug(f"Failed to parse resolution time from title '{title}': {e}")
        return None


def get_market_id_from_url(event_url: str) -> Optional[str]:
    """
    Fetch market ID (conditionId) from a Polymarket event URL.
    
    This function attempts to extract the market ID by:
    1. Parsing the URL slug to find the market
    2. Querying Polymarket's data API to find matching markets
    
    Args:
        event_url: Polymarket event URL (e.g., https://polymarket.com/event/solana-up-or-down-december-3-6am-et)
    
    Returns:
        Market ID (conditionId) if found, None otherwise
    """
    try:
        # Extract slug from URL
        if "/event/" not in event_url:
            logger.warning(f"Invalid Polymarket URL format: {event_url}")
            return None
        
        slug = event_url.split("/event/")[-1].split("?")[0].split("#")[0]
        logger.info(f"Looking up market ID for slug: {slug}")
        
        # Query Polymarket markets API to find the market
        # Note: This is a simplified approach - Polymarket's API structure may vary
        # In practice, you may need to query their markets endpoint or scrape the page
        endpoint = f"{DATA_API_BASE_URL}/markets"
        params = {"slug": slug}
        
        session = get_http_session()
        response = session.get(endpoint, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list) and len(data) > 0:
                market_id = data[0].get("conditionId") or data[0].get("id")
                if market_id:
                    logger.info(f"Found market ID {market_id} for slug {slug}")
                    return market_id
        
        logger.warning(f"Could not find market ID for URL: {event_url}")
        return None
        
    except Exception as e:
        logger.error(f"Error fetching market ID from URL {event_url}: {e}")
        return None


def get_market_tokens(market_id: str) -> Dict[str, str]:
    """
    Get both Up and Down token IDs for a market.
    
    Tries multiple approaches:
    1. Gamma API by market slug (if we can derive slug from market_id)
    2. CLOB API /markets endpoint - search all markets
    3. Data API with conditionId parameter (fallback)
    
    Args:
        market_id: Market conditionId (with or without 0x prefix)
    
    Returns:
        Dict with "UP" and "DOWN" token IDs, or empty dict if not found
    """
    session = get_http_session()
    
    # Normalize market_id (ensure it has 0x prefix)
    normalized_market_id = market_id.lower()
    if not normalized_market_id.startswith("0x"):
        normalized_market_id = "0x" + normalized_market_id
    
    # Try CLOB API first - search through all markets (most comprehensive)
    try:
        endpoint = f"{CLOB_API_BASE_URL}/markets"
        response = session.get(endpoint, timeout=20)  # Increased timeout for large response
        
        if response.status_code == 200:
            data = response.json()
            markets = data.get("data", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
            
            logger.debug(f"Searching {len(markets)} markets for condition_id {normalized_market_id[:20]}...")
            
            # Find market with matching condition_id
            for market in markets:
                condition_id = market.get("condition_id", "").lower()
                if condition_id == normalized_market_id:
                    tokens = market.get("tokens", [])
                    result = {}
                    for token in tokens:
                        outcome = token.get("outcome", "").upper()
                        token_id = token.get("token_id") or token.get("tokenId") or token.get("id")
                        if outcome in ("UP", "DOWN") and token_id:
                            result[outcome] = str(token_id)
                    
                    if result:
                        logger.info(f"Found {len(result)} tokens for market {market_id} via CLOB API: {list(result.keys())}")
                        return result
                    else:
                        logger.warning(f"Found market {market_id} but no Up/Down tokens in tokens array (has {len(tokens)} tokens)")
    except Exception as e:
        logger.debug(f"CLOB API failed for {market_id}: {e}")
    
    # Fallback: Try Data API
    try:
        endpoint = f"{DATA_API_BASE_URL}/markets"
        params = {"conditionId": normalized_market_id}
        response = session.get(endpoint, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            markets = data if isinstance(data, list) else (data.get("data", []) if isinstance(data, dict) else [])
            
            if markets and len(markets) > 0:
                market = markets[0]
                tokens = market.get("tokens", [])
                result = {}
                for token in tokens:
                    outcome = token.get("outcome", "").upper()
                    token_id = token.get("tokenId") or token.get("token_id") or token.get("id")
                    if outcome in ("UP", "DOWN") and token_id:
                        result[outcome] = str(token_id)
                
                if result:
                    logger.info(f"Found {len(result)} tokens for market {market_id} via Data API: {list(result.keys())}")
                    return result
    except Exception as e:
        logger.debug(f"Data API failed for {market_id}: {e}")
    
    # Last resort: Try Gamma API (may not have tokens but can help with market info)
    # Note: Gamma API might not return tokens, but we can try
    try:
        # We'd need the market slug, which we don't have from conditionId alone
        # Skip Gamma API for now as it requires slug
        pass
    except Exception as e:
        logger.debug(f"Gamma API not attempted: {e}")
    
    logger.warning(f"Could not find tokens for market {market_id} in either API")
    return {}


def get_market_tokens_by_token_id(token_id: str) -> Dict[str, str]:
    """
    Find a market by one of its token IDs and return both Up and Down tokens.
    
    This is useful when we only have one token ID from a position and need to find the other.
    Uses CLOB API /markets endpoint to search through all markets.
    
    Args:
        token_id: One token ID from the market
    
    Returns:
        Dict with "UP" and "DOWN" token IDs, or empty dict if not found
    """
    session = get_http_session()
    
    try:
        # Use CLOB API - search through all markets for one containing this token
        endpoint = f"{CLOB_API_BASE_URL}/markets"
        response = session.get(endpoint, timeout=20)  # Increased timeout
        
        if response.status_code == 200:
            data = response.json()
            markets = data.get("data", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
            
            logger.debug(f"Searching {len(markets)} markets for token_id {token_id[:20]}...")
            
            # Search for market containing this token_id
            for market in markets:
                tokens = market.get("tokens", [])
                found_token = False
                result = {}
                
                for token in tokens:
                    t_id = str(token.get("token_id") or token.get("tokenId") or token.get("id", ""))
                    outcome = token.get("outcome", "").upper()
                    
                    if t_id == str(token_id):
                        found_token = True
                    
                    # Collect all Up/Down tokens from this market
                    if outcome in ("UP", "DOWN") and t_id:
                        result[outcome] = t_id
                
                if found_token:
                    if result and len(result) >= 1:  # At least one token found
                        logger.info(f"Found market by token_id {token_id[:20]}... with {len(result)} tokens: {list(result.keys())}")
                        return result
                    else:
                        logger.warning(f"Found market with token_id {token_id[:20]}... but no Up/Down tokens extracted")
    except Exception as e:
        logger.debug(f"Error finding market by token_id {token_id[:20]}...: {e}")
    
    return {}


def get_market_prices(token_ids: List[str]) -> Dict[str, Dict[str, float]]:
    """
    Fetch live prices for multiple tokens from Polymarket CLOB API.
    
    Uses GET /prices endpoint: https://clob.polymarket.com/prices
    Note: This endpoint returns ALL prices, we filter by token_ids
    
    Args:
        token_ids: List of token IDs (Asset IDs) to get prices for
    
    Returns:
        Dict mapping token_id -> {"BUY": price, "SELL": price}
        Returns empty dict if API call fails
    """
    if not token_ids:
        return {}
    
    endpoint = f"{CLOB_API_BASE_URL}/prices"
    session = get_http_session()
    
    try:
        # The /prices endpoint returns ALL prices (no params needed)
        # We'll filter by our token_ids
        response = session.get(endpoint, timeout=10)
        
        if response.status_code == 200:
            all_prices = response.json()
            # Filter to only return prices for our token_ids
            filtered_prices = {}
            token_id_set = set(token_ids)
            
            for token_id, prices in all_prices.items():
                if token_id in token_id_set:
                    # Convert string prices to float
                    buy_price = float(prices.get("BUY", 0.0))
                    sell_price = float(prices.get("SELL", 0.0))
                    filtered_prices[token_id] = {
                        "BUY": buy_price,
                        "SELL": sell_price
                    }
            
            logger.info(f"Fetched prices for {len(filtered_prices)}/{len(token_ids)} tokens")
            return filtered_prices
        else:
            logger.warning(f"Failed to fetch prices: HTTP {response.status_code} - {response.text[:200]}")
            return {}
    except Exception as e:
        logger.error(f"Error fetching market prices: {e}", exc_info=True)
        return {}


def get_market_price(token_id: str, side: str = "BUY") -> Optional[float]:
    """
    Fetch live price for a single token from Polymarket CLOB API.
    
    Uses GET /price endpoint: https://clob.polymarket.com/price
    
    Args:
        token_id: Token ID (Asset ID) to get price for
        side: "BUY" or "SELL" (default: "BUY")
    
    Returns:
        Current price (0-1 range) or None if API call fails
    """
    endpoint = f"{CLOB_API_BASE_URL}/price"
    session = get_http_session()
    
    try:
        params = {"token_id": token_id, "side": side.upper()}
        response = session.get(endpoint, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            # API returns: {"price": float} or {"error": "..."}
            if "error" in data:
                logger.warning(f"API error for {token_id}: {data.get('error')}")
                return None
            price = data.get("price")
            if price is not None:
                return float(price)
            return None
        else:
            error_text = response.text[:200] if hasattr(response, 'text') else ""
            logger.warning(f"Failed to fetch price for {token_id}: HTTP {response.status_code} - {error_text}")
            return None
    except Exception as e:
        logger.error(f"Error fetching price for {token_id}: {e}")
        return None


def fetch_trades_in_range(
    target_wallet: str,
    start_timestamp: int,
    end_timestamp: int
) -> list[Trade]:
    """
    Fetch all trades for the target wallet(s) within a time range.
    Uses pagination to fetch all trades (not just the first page).
    Now supports multiple target wallets - checks all configured TARGET_WALLETS.
    
    Args:
        target_wallet: Primary proxy wallet address (for backward compatibility)
        start_timestamp: Unix timestamp to fetch trades from (inclusive)
        end_timestamp: Unix timestamp to fetch trades until (exclusive)
    
    Returns:
        List of Trade objects sorted by timestamp (oldest first) from all target wallets
    """
    # Fetch trades from all target wallets
    all_trades = []
    for wallet in TARGET_WALLETS:
        wallet_trades = _fetch_trades_in_range_single(wallet, start_timestamp, end_timestamp)
        all_trades.extend(wallet_trades)
    
    # Sort by timestamp and remove duplicates by transaction hash
    seen_hashes = set()
    unique_trades = []
    for trade in sorted(all_trades, key=lambda t: t.timestamp):
        if trade.transaction_hash and trade.transaction_hash not in seen_hashes:
            seen_hashes.add(trade.transaction_hash)
            unique_trades.append(trade)
    
    logger.info(f"Fetched {len(unique_trades)} unique trades from {len(TARGET_WALLETS)} target wallet(s)")
    return unique_trades


def _fetch_trades_in_range_single(
    target_wallet: str,
    start_timestamp: int,
    end_timestamp: int
) -> list[Trade]:
    """
    Fetch all trades for a single target wallet within a time range.
    Uses pagination to fetch all trades (not just the first page).
    
    Args:
        target_wallet: Proxy wallet address to monitor
        start_timestamp: Unix timestamp to fetch trades from (inclusive)
        end_timestamp: Unix timestamp to fetch trades until (exclusive)
    
    Returns:
        List of Trade objects sorted by timestamp (oldest first)
    """
    endpoint = f"{DATA_API_BASE_URL}/activity"
    session = get_http_session()
    retry_delay = INITIAL_RETRY_DELAY
    
    all_trades = []
    seen_hashes = set()
    page = 0
    max_pages = 50  # Safety limit to prevent infinite loops
    
    logger.info(f"Fetching all trades from Polymarket API for range {start_timestamp}-{end_timestamp}")
    
    while page < max_pages:
        params = {
            "user": target_wallet.lower(),
            "type": "TRADE",
            "sortBy": "TIMESTAMP",
            "sortDirection": "DESC",
            "limit": 100,  # API limit per page
            "offset": page * 100  # Pagination offset
        }
        
        for attempt in range(MAX_RETRIES):
            try:
                logger.info(f"Fetching page {page + 1} (offset {params['offset']})...")
                response = session.get(endpoint, params=params, timeout=15)
                
                if response.status_code == 429:
                    # Rate limit hit - Cloudflare throttling
                    # Data API limits: 200 req/10s or 75 req/10s for /trades endpoint
                    retry_after = response.headers.get('Retry-After')
                    if retry_after:
                        wait_time = int(retry_after)
                        logger.warning(f"Rate limit hit (429). Retry-After header: {wait_time}s")
                    else:
                        wait_time = retry_delay
                        logger.warning(f"Rate limit hit (429). Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                    retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY)
                    continue
                
                response.raise_for_status()
                activities = response.json()
                
                if not activities:
                    # No more results
                    logger.info(f"No more activities on page {page + 1}, stopping pagination")
                    page = max_pages  # Break outer loop
                    break
                
                logger.info(f"Page {page + 1}: API returned {len(activities)} activities")
                
                # Parse trades from this page
                page_trades = []
                skipped_before = 0
                skipped_after = 0
                found_in_range = 0
                
                for activity in activities:
                    tx_hash = activity.get("transactionHash", "")
                    activity_timestamp = activity.get("timestamp", 0)
                    
                    # Skip if no hash or duplicate
                    if not tx_hash or tx_hash in seen_hashes:
                        continue
                    
                    seen_hashes.add(tx_hash)
                    
                    # Filter by timestamp range
                    if activity_timestamp < start_timestamp:
                        skipped_before += 1
                        # If we're getting timestamps before our range, we can stop paginating
                        # (since results are sorted DESC, all future pages will be older)
                        if page > 0:  # Only stop if we've checked at least one page
                            logger.info(f"Reached timestamps before range, stopping pagination")
                            page = max_pages
                            break
                        continue
                    if activity_timestamp >= end_timestamp:
                        skipped_after += 1
                        continue
                    
                    found_in_range += 1
                    
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
                            page_trades.append(trade)
                        else:
                            logger.warning(f"Missing market/token ID: {trade}")
                    except (ValueError, KeyError) as e:
                        logger.warning(f"Failed to parse trade activity: {e}")
                        continue
                
                all_trades.extend(page_trades)
                logger.info(f"Page {page + 1}: Found {found_in_range} trades in range "
                           f"(skipped {skipped_before} before, {skipped_after} after, "
                           f"total so far: {len(all_trades)})")
                
                # Determine if we should continue paginating
                # Since results are sorted DESC (newest first):
                # - If we see timestamps AFTER our range, we need to keep going (older results coming)
                # - If we see timestamps BEFORE our range, we can stop (all future pages will be older)
                # - If we get fewer results than limit, we've reached the end
                
                if len(activities) < params["limit"]:
                    logger.info(f"Reached end of results (got {len(activities)} < limit {params['limit']})")
                    page = max_pages  # Break outer loop
                    break
                
                # Check timestamps on this page
                timestamps = [a.get("timestamp", 0) for a in activities if a.get("timestamp", 0)]
                if timestamps:
                    min_ts = min(timestamps)
                    max_ts = max(timestamps)
                    logger.debug(f"Page {page + 1} timestamp range: {min_ts} to {max_ts} "
                               f"({datetime.fromtimestamp(min_ts)} to {datetime.fromtimestamp(max_ts)})")
                    
                    # If all timestamps are before our range, stop (results are DESC sorted)
                    if max_ts < start_timestamp:
                        logger.info(f"All timestamps on page {page + 1} are before range ({max_ts} < {start_timestamp}), stopping")
                        page = max_pages  # Break outer loop
                        break
                    
                    # If we have timestamps after our range, we need to keep going
                    # to find trades that fall within our range
                    if min_ts >= end_timestamp:
                        logger.info(f"All timestamps on page {page + 1} are after range ({min_ts} >= {end_timestamp}), continuing...")
                        # Continue to next page
                        page += 1
                        break
                    
                    # Mixed timestamps or some in range - continue
                    logger.info(f"Page {page + 1} has mixed timestamps, continuing to next page...")
                
                # Continue to next page
                page += 1
                break  # Success, break retry loop
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed (page {page + 1}, attempt {attempt + 1}/{MAX_RETRIES}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY)
                else:
                    logger.error(f"Max retries reached for page {page + 1}, stopping pagination")
                    page = max_pages  # Break outer loop
                    break
        else:
            # If we broke out of the retry loop without success, stop pagination
            break
    
    # Sort by timestamp (oldest first) so we process trades in chronological order
    all_trades.sort(key=lambda t: t.timestamp)
    logger.info(f"Total: Found {len(all_trades)} trades in range {start_timestamp}-{end_timestamp} "
               f"across {page} pages")
    if len(all_trades) > 0:
        logger.info(f"Trade timestamps: first={all_trades[0].timestamp} ({datetime.fromtimestamp(all_trades[0].timestamp)}), "
                   f"last={all_trades[-1].timestamp} ({datetime.fromtimestamp(all_trades[-1].timestamp)})")
    else:
        logger.warning(f"No trades found in range! Check if timestamps are correct.")
        logger.warning(f"Range: {start_timestamp} ({datetime.fromtimestamp(start_timestamp)}) to "
                      f"{end_timestamp} ({datetime.fromtimestamp(end_timestamp)})")
    return all_trades


def fetch_trades_since(
    target_wallet: str,
    last_timestamp: int,
    seen_hashes: set[str]
) -> list[Trade]:
    """
    Fetch new trades for the target wallet since last_timestamp.
    
    Now supports multiple target wallets - checks all configured TARGET_WALLETS.

    Uses Polymarket Data API /activity endpoint:
    https://docs.polymarket.com/developers/misc-endpoints/data-api-activity

    Args:
        target_wallet: Primary proxy wallet address to monitor (for backward compatibility)
        last_timestamp: Unix timestamp to fetch trades after
        seen_hashes: Set of already-seen transaction hashes (for deduplication)

    Returns:
        List of new Trade objects (not in seen_hashes) from all target wallets
    """
    # Fetch trades from all target wallets
    all_trades = []
    for wallet in TARGET_WALLETS:
        wallet_trades = _fetch_trades_since_single(wallet, last_timestamp, seen_hashes)
        all_trades.extend(wallet_trades)
    
    # Sort by timestamp and remove duplicates
    all_trades.sort(key=lambda t: t.timestamp)
    return all_trades


def _fetch_trades_since_single(
    target_wallet: str,
    last_timestamp: int,
    seen_hashes: set[str]
) -> list[Trade]:
    """
    Fetch new trades for a single target wallet since last_timestamp.

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

            # Handle rate limiting (Cloudflare throttling)
            # Data API limits: 200 req/10s general, 75 req/10s for /trades endpoint
            if response.status_code == 429:
                retry_after = response.headers.get('Retry-After')
                if retry_after:
                    wait_time = int(retry_after)
                    logger.warning(f"Rate limit hit (429). Retry-After header: {wait_time}s")
                else:
                    wait_time = retry_delay
                    logger.warning(f"Rate limit hit (429). Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
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
                    if not market_id:
                        market_id = activity.get("conditionId", "")
                    
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

    Filters by:
    - Basic validation (size, price, market/token IDs)
    - Allowed market IDs (if configured)
    - Market title patterns (if market IDs not configured)

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

    # Filter by market ID if configured
    if ALLOWED_MARKET_IDS:
        market_id_lower = trade.market_id.lower()
        if market_id_lower not in ALLOWED_MARKET_IDS:
            logger.debug(f"Trade filtered out: market_id {trade.market_id} not in allowed list")
            return False
        logger.debug(f"Trade allowed: market_id {trade.market_id} is in allowed list")
        return True
    
    # If no market IDs configured, filter by title pattern
    if trade.title:
        title_lower = trade.title.lower()
        # Check if title matches any allowed pattern
        matches_pattern = any(pattern in title_lower for pattern in ALLOWED_MARKET_PATTERNS)
        if not matches_pattern:
            logger.debug(f"Trade filtered out: title '{trade.title}' doesn't match allowed patterns")
            return False
        logger.debug(f"Trade allowed: title '{trade.title}' matches allowed pattern")
        return True
    
    # If no title available and no market IDs configured, allow all trades
    logger.warning(f"No market filtering possible: market_id={trade.market_id}, title={trade.title}")
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
    Place a copy order on Polymarket via CLOB (Central Limit Order Book) API.

    CLOB System Architecture:
    - Hybrid-decentralized: Off-chain order matching/ordering with on-chain settlement
    - Non-custodial: Settlement executed on-chain via signed order messages (EIP712)
    - Orders are EIP712-signed structured data
    - Matched orders have one maker and one or more takers
    - Price improvements benefit the taker
    - Operator handles off-chain order management and submits matched trades on-chain
    
    Security:
    - Exchange contract audited by Chainsecurity
    - Operator privileges limited to order matching (non-censorship, correct ordering)
    - Operators cannot set prices or execute unauthorized trades
    - Users can cancel orders on-chain independently if needed
    
    Fees:
    - Currently 0 bps for both maker and taker (subject to change)
    - Fees apply symmetrically in output assets (proceeds)
    
    Uses py-clob-client to sign and submit orders to the CLOB.
    The CLOB is Polymarket's off-chain order matching system where resting orders
    and market orders are matched before being sent on-chain to Polygon Network.
    
    Endpoint: https://clob.polymarket.com/ (REST CLOB API)
    Documentation: https://docs.polymarket.com/developers/CLOB/orders/create-order
    py-clob-client: https://github.com/Polymarket/py-clob-client

    Args:
        trade: The original trade to copy (contains token_id/Asset ID, price, size, side)
        my_size: The size (number of tokens/shares) for our order
        clob_client: Initialized ClobClient instance

    Returns:
        Order ID if successful, None if failed
    
    Trade Lifecycle:
    After placing an order, trades go through these statuses:
    - MATCHED: Trade matched and sent to executor service (non-terminal)
    - MINED: Trade observed to be mined into chain (non-terminal)
    - CONFIRMED: Trade achieved strong probabilistic finality (terminal, success)
    - RETRYING: Trade transaction failed and being retried (non-terminal)
    - FAILED: Trade failed and not being retried (terminal, failure)
    
    Note: Large trades may be split into multiple transactions (reconciled by
    market_order_id, match_time, and bucket_index). The bot places orders but
    does not actively track trade statuses - orders are assumed to execute.
    
    To verify order execution, use CLOB API GET /data/trades endpoint:
    - Endpoint: GET /data/trades (requires L2 Header authentication)
    - Filter by: taker (your address), market, before/after timestamps
    - Returns Trade objects with: id, status, match_time, transaction_hash, bucket_index, etc.
    - Trade statuses: MATCHED, MINED, CONFIRMED (success), RETRYING, FAILED (failure)
    """
    try:
        # Map side to CLOB constants
        side = BUY if trade.side == "BUY" else SELL

        # Create order arguments
        # token_id (Asset ID) represents the specific Yes/No outcome token for this market
        # Price is 0-1 range, where winning tokens resolve to $1 USDC
        order_args = OrderArgs(
            price=trade.price,
            size=my_size,
            side=side,
            token_id=trade.token_id  # Token ID (Asset ID) for the outcome
        )

        logger.info(f"Placing order: {side} {my_size:.2f} @ ${trade.price:.4f} "
                   f"(${my_size * trade.price:.2f} USDC)")

        # Create and sign the order
        signed_order = clob_client.create_order(order_args)
        
        # Submit order to CLOB as GTC (Good-Till-Cancelled) order
        response = clob_client.post_order(signed_order, OrderType.GTC)
        
        # Extract order ID from response
        order_id = response.get("orderID") or response.get("order_id") or signed_order.get("orderID") or "unknown"

        logger.info(f"✅ Order placed successfully! Order ID: {order_id}")
        return order_id

    except Exception as e:
        error_msg = str(e).lower()

        # Handle common errors gracefully
        if "insufficient balance" in error_msg or "insufficient funds" in error_msg:
            logger.error(f"❌ Insufficient balance (need ~${my_size * trade.price:.2f} USDC)")
        elif "allowance" in error_msg:
            logger.error(f"❌ Insufficient allowance. Set USDC allowance on Polymarket website.")
        elif "rate limit" in error_msg or "429" in error_msg or "throttle" in error_msg:
            # CLOB POST /order limits: 2400 req/10s burst, 24000 req/10min sustained (40 req/s)
            # With default POLL_INTERVAL_SECONDS=2.0, we make ~0.5 orders/second, well within limits
            logger.warning(f"⚠️ Rate limit hit on CLOB API. Order will be retried on next poll cycle.")
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
            
            # Make action very clear
            if trade.side.upper() == "BUY":
                action_emoji = "🟢"
                action_text = "🟢 BUYING"
            else:
                action_emoji = "🔴"
                action_text = "🔴 SELLING"
            
            # Generate URLs for hyperlinks
            trader_analytics_url = trade.get_trader_analytics_url(notifier.target_wallet)
            tx_url = trade.get_transaction_url()
            
            trade_notification = (
                f"🔔 {action_text} {coin_name} {price_change}\n"
                f"{trade.title}\n\n"
                f"👤 Original: {trade.side} {trade.size:.1f} @ ${trade.price:.4f} (${trade.usdc_value():.1f})\n"
                f"🤖 Copy: {my_size:.1f} @ ${trade.price:.4f} (${my_value:.1f}) {risk_multiplier}x"
            )
            
            # Add links if available
            links = []
            if trader_analytics_url:
                links.append(f"[View Trader Analytics]({trader_analytics_url})")
            if tx_url:
                links.append(f"[View Transaction]({tx_url})")
            
            if links:
                trade_notification += f"\n\n🔗 " + " | ".join(links)
            
            notifier.send_message_to_all(trade_notification, parse_mode='Markdown')
        
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

    # Check if trading is enabled (either via DRY_RUN env or Telegram LIVE/STOP)
    # If Telegram is enabled, check the trading state
    should_place_order = not dry_run
    if notifier.enabled:
        should_place_order = should_place_order and notifier.is_live_trading
    
    if not should_place_order:
        if dry_run:
            logger.info("🏃 DRY RUN MODE: Not placing real order")
        elif notifier.enabled and not notifier.is_live_trading:
            logger.info("🔴 STOPPED MODE: Trading is disabled. Not placing real order (draft mode)")
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

    # Track seen resolution IDs to avoid duplicates (now handled in BotState.__post_init__)

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
    if len(TARGET_WALLETS) > 1:
        logger.info(f"  Target Wallets ({len(TARGET_WALLETS)}):")
        for i, wallet in enumerate(TARGET_WALLETS, 1):
            logger.info(f"    {i}. {wallet}")
    else:
        logger.info(f"  Target Wallet: {target_wallet}")
    if ALLOWED_MARKET_IDS:
        logger.info(f"  Allowed Markets: {len(ALLOWED_MARKET_IDS)} market ID(s)")
    else:
        logger.info(f"  Allowed Markets: Filtering by patterns ({len(ALLOWED_MARKET_PATTERNS)} patterns)")
        logger.info(f"    Patterns: {', '.join(ALLOWED_MARKET_PATTERNS)}")
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

            # Initialize client based on signature type
            # Reference: https://docs.polymarket.com/developers/CLOB/clob-introduction#clients
            # chain_id=137 is Polygon Network mainnet (where Polymarket operates)
            if SIGNATURE_TYPE == 0:
                # EOA (Externally Owned Account) - direct wallet trading
                # ClobClient(host, key=key, chain_id=chain_id)
                clob_client = ClobClient(
                    host=CLOB_API_BASE_URL,
                    key=private_key,
                    chain_id=137
                )
                logger.info("Initialized Polymarket CLOB client (EOA mode)")
            elif SIGNATURE_TYPE == 1 and POLYMARKET_PROXY_ADDRESS:
                # Email/Magic account - requires proxy address
                # ClobClient(host, key=key, chain_id=chain_id, signature_type=1, funder=POLYMARKET_PROXY_ADDRESS)
                clob_client = ClobClient(
                    host=CLOB_API_BASE_URL,
                    key=private_key,
                    chain_id=137,
                    signature_type=1,
                    funder=POLYMARKET_PROXY_ADDRESS
                )
                logger.info(f"Initialized Polymarket CLOB client (Email/Magic, proxy={POLYMARKET_PROXY_ADDRESS[:10]}...)")
            elif SIGNATURE_TYPE == 2 and POLYMARKET_PROXY_ADDRESS:
                # Browser Wallet (Metamask, Coinbase Wallet, etc) - requires proxy address
                # ClobClient(host, key=key, chain_id=chain_id, signature_type=2, funder=POLYMARKET_PROXY_ADDRESS)
                clob_client = ClobClient(
                    host=CLOB_API_BASE_URL,
                    key=private_key,
                    chain_id=137,
                    signature_type=2,
                    funder=POLYMARKET_PROXY_ADDRESS
                )
                logger.info(f"Initialized Polymarket CLOB client (Browser Wallet, proxy={POLYMARKET_PROXY_ADDRESS[:10]}...)")
            else:
                logger.error("Invalid configuration: SIGNATURE_TYPE 1 or 2 requires POLYMARKET_PROXY_ADDRESS")
                sys.exit(1)
            
            # Set API credentials (required before placing orders)
            if clob_client is not None:
                clob_client.set_api_creds(clob_client.create_or_derive_api_creds())
                logger.info("✅ API credentials set for CLOB client")

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
