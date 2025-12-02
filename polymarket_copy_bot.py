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
from dataclasses import dataclass
from typing import Optional
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

    def save(self, filepath: Path) -> None:
        """Save state to JSON file."""
        try:
            data = {
                "last_seen_timestamp": self.last_seen_timestamp,
                "seen_transaction_hashes": list(self.seen_transaction_hashes)
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
                    seen_transaction_hashes=set(data.get("seen_transaction_hashes", []))
                )
                logger.info(f"Loaded state: {len(state.seen_transaction_hashes)} transactions tracked")
                return state
            except Exception as e:
                logger.warning(f"Failed to load state: {e}. Starting fresh.")

        logger.info("Starting with fresh state")
        return BotState(
            last_seen_timestamp=int(time.time()),
            seen_transaction_hashes=set()
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
    dry_run: bool
) -> None:
    """
    Process a single detected trade: validate, size, and place copy order.

    Args:
        trade: The Trade to process
        clob_client: CLOB client for order placement (None in dry run)
        risk_multiplier: Risk multiplier for sizing
        max_usdc: Max USDC per trade
        dry_run: If True, don't place real orders
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

    # Dry run mode
    if dry_run:
        logger.info("🏃 DRY RUN MODE: Not placing real order")
        return

    # Place the copy order
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
    dry_run: bool
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
                process_trade(trade, clob_client, risk_multiplier, max_usdc, dry_run)

                # Update state timestamp
                if trade.timestamp > state.last_seen_timestamp:
                    state.last_seen_timestamp = trade.timestamp

                # Save state after each trade
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
        state.save(STATE_FILE)


def print_banner(
    target_wallet: str,
    max_usdc: float,
    risk_multiplier: float,
    poll_interval: float,
    dry_run: bool,
    state: BotState
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
    logger.info("")
    logger.info("State:")
    logger.info(f"  Tracked transactions: {len(state.seen_transaction_hashes)}")
    logger.info(f"  Last seen: {datetime.fromtimestamp(state.last_seen_timestamp)}")
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
        state
    )

    # Start main loop
    main_loop(
        target_wallet=TARGET_WALLET,
        clob_client=clob_client,
        state=state,
        poll_interval=POLL_INTERVAL_SECONDS,
        risk_multiplier=RISK_MULTIPLIER,
        max_usdc=MAX_TRADE_USDC,
        dry_run=DRY_RUN
    )


if __name__ == "__main__":
    main()
