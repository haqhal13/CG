# Polymarket Copy Trading Bot

A production-ready Python bot that automatically copies trades from a target wallet on Polymarket in near real-time.

## ⚠️ IMPORTANT WARNINGS

**FINANCIAL RISK**: This bot trades with real money. Automated trading carries significant financial risk including:
- Complete loss of invested capital
- Market volatility
- API errors or delays
- Network issues
- Bugs in code

**Only use funds you can afford to lose completely.**

**SECURITY**:
- Never share your private key with anyone
- Never commit your `.env` file to version control
- Keep your private key secure at all times

## Features

✅ **Real-time Trade Monitoring**: Polls Polymarket Data API every 2-3 seconds for minimal latency
✅ **Automatic Trade Copying**: Instantly replicates trades (same market, side, outcome, size, price)
✅ **Risk Management**: Configurable position sizing with max trade limits
✅ **State Persistence**: Prevents duplicate trades across bot restarts
✅ **Exponential Backoff**: Handles network errors and rate limits gracefully
✅ **Dry Run Mode**: Test the bot without placing real orders
✅ **Comprehensive Logging**: Detailed logs for debugging and monitoring
✅ **Telegram Status & Alerts**: Get notifications for copied trades, open positions, and realized profits directly in Telegram

## Target Wallet

Currently configured to copy trades from:
```
0xeffcc79a8572940cee2238b44eac89f2c48fda88
```

## Requirements

- Python 3.8+
- Active Polymarket account with funded wallet
- Ethereum/Polygon private key
- USDC balance for trading

## Installation

1. **Clone or download this repository**

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure the bot**:
   ```bash
   # Copy the example environment file
   cp .env.example .env

   # Edit .env and add your private key and settings
   # On Windows: copy .env.example .env
   ```

4. **Edit `.env` file** with your configuration:
   ```env
   POLYMARKET_PRIVATE_KEY=your_private_key_without_0x_prefix
   MAX_TRADE_USDC=100.0
   RISK_MULTIPLIER=1.0
   POLL_INTERVAL_SECONDS=2.0
   DRY_RUN=false
   LOG_LEVEL=INFO
   ```

## Configuration Options

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `POLYMARKET_PRIVATE_KEY` | Your Ethereum private key (required, no 0x prefix) | None |
| `MAX_TRADE_USDC` | Maximum USDC to risk per trade | 100.0 |
| `RISK_MULTIPLIER` | Position size multiplier (1.0 = exact copy) | 1.0 |
| `POLL_INTERVAL_SECONDS` | Polling frequency in seconds | 2.0 |
| `DRY_RUN` | Test mode (true/false) | false |
| `LOG_LEVEL` | Logging verbosity (DEBUG/INFO/WARNING/ERROR) | INFO |
| `TELEGRAM_BOT_TOKEN` | Telegram bot token for alerts (leave empty to disable) | None |
| `TELEGRAM_CHAT_ID` | Telegram chat/user ID to notify | None |

### Risk Management

- **MAX_TRADE_USDC**: Caps individual trade size to prevent over-exposure
- **RISK_MULTIPLIER**:
  - `1.0` = Copy exact same size as target wallet
  - `0.5` = Use half the size (more conservative)
  - `2.0` = Use double the size (more aggressive)

## Telegram Integration

Stay in sync with the bot even when you are away from your terminal.

1. Create a Telegram bot with [@BotFather](https://t.me/botfather) and copy the token.
2. Determine the chat ID (personal DM or group) using [@userinfobot](https://t.me/userinfobot) or similar.
3. Set `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` in your `.env`.

Once configured the bot will:
- Notify you when trades are copied, positions are closed, or profits are realized
- Track open exposure and closed trades with PnL
- Respond to the following commands:
  - `/status` – quick health summary and last closed trade
  - `/openpositions` – list of active positions
  - `/closedpositions` – five most recent closed trades
  - `/pnl` – cumulative realized profit & loss

## Usage

### Dry Run Mode (Recommended First)

Test the bot without placing real orders:

```bash
# Set DRY_RUN=true in .env file
python polymarket_copy_bot.py
```

The bot will detect trades and log what it would do, but won't place real orders.

### Live Trading Mode

Once you're confident, set `DRY_RUN=false` and run:

```bash
python polymarket_copy_bot.py
```

The bot will:
1. Load previous state from `bot_state.json` (if exists)
2. Start monitoring the target wallet
3. Automatically copy any new trades
4. Log all activity to console and `polymarket_bot.log`

### Stop the Bot

Press `Ctrl+C` to gracefully stop the bot. It will save its state before exiting.

## How It Works

### Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Copy Trading Bot                      │
├─────────────────────────────────────────────────────────┤
│                                                           │
│  ┌─────────────────┐         ┌────────────────────┐    │
│  │  Data Fetcher   │         │   Order Placer     │    │
│  │                 │         │                    │    │
│  │ - Polls Data API│         │ - Uses CLOB API    │    │
│  │ - Parses trades │         │ - Signs orders     │    │
│  │ - Deduplicates  │         │ - Places trades    │    │
│  └────────┬────────┘         └────────┬───────────┘    │
│           │                           │                 │
│           └──────────┬────────────────┘                 │
│                      │                                   │
│              ┌───────▼────────┐                         │
│              │   Bot State    │                         │
│              │                │                         │
│              │ - Timestamps   │                         │
│              │ - Seen trades  │                         │
│              │ - JSON persist │                         │
│              └────────────────┘                         │
└─────────────────────────────────────────────────────────┘
```

### Trade Copying Process

1. **Detection**: Bot polls Polymarket Data API every 2 seconds
2. **Parsing**: Extracts trade details (market, side, size, price, outcome)
3. **Deduplication**: Checks if trade was already processed
4. **Sizing**: Applies risk multiplier and max trade cap
5. **Execution**: Places equivalent order via CLOB API
6. **Persistence**: Saves state to prevent re-processing

## Files Generated

- **`bot_state.json`**: Persistent state (timestamps, seen trades)
- **`polymarket_bot.log`**: Detailed log file
- **`.env`**: Your configuration (DO NOT commit to git!)

## API Documentation

- [Polymarket Data API](https://docs.polymarket.com/developers/misc-endpoints/data-api-activity)
- [Polymarket CLOB API](https://docs.polymarket.com/developers/CLOB/orders/create-order)
- [py-clob-client](https://github.com/Polymarket/py-clob-client)

## Troubleshooting

### "POLYMARKET_PRIVATE_KEY environment variable is required"

Make sure you:
1. Created a `.env` file (copy from `.env.example`)
2. Added your private key without the `0x` prefix
3. The `.env` file is in the same directory as the script

### "Insufficient balance to place order"

Your wallet doesn't have enough USDC. Either:
- Deposit more USDC to your wallet
- Lower `MAX_TRADE_USDC` in `.env`
- Lower `RISK_MULTIPLIER` in `.env`

### "Insufficient allowance"

You need to approve USDC spending:
1. Visit [Polymarket](https://polymarket.com)
2. Connect your wallet
3. Set allowances for the Exchange contract
4. Try again

### "Rate limit hit"

The bot is making too many requests. The bot handles this automatically with exponential backoff, but you can:
- Increase `POLL_INTERVAL_SECONDS` in `.env`
- Wait a few minutes and restart

### No trades detected

This is normal if the target wallet isn't actively trading. The bot will keep monitoring until a trade occurs.

## Security Best Practices

1. **Never commit `.env` file** - Add it to `.gitignore`
2. **Use a dedicated trading wallet** - Don't use your main wallet
3. **Start with small amounts** - Test with minimal funds first
4. **Monitor regularly** - Check logs frequently when starting out
5. **Use DRY_RUN first** - Always test before live trading
6. **Keep software updated** - Regularly update dependencies

## Limitations

- **Execution delay**: 2-3 second polling means trades may execute slightly after target
- **Slippage**: Market prices can change between detection and execution
- **API dependencies**: Relies on Polymarket API availability
- **Gas fees**: Polygon gas fees apply to each trade
- **Market liquidity**: Large trades may not fill completely

## License

This software is provided as-is for educational purposes. Use at your own risk.

## Support

For issues or questions:
- Check the logs in `polymarket_bot.log`
- Review Polymarket API documentation
- Verify your configuration in `.env`

## Disclaimer

This bot is for educational and research purposes. Cryptocurrency trading carries significant financial risk. The authors are not responsible for any financial losses incurred through the use of this software. Always trade responsibly and never invest more than you can afford to lose.
