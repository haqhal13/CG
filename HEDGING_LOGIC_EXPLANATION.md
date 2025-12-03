# How Bot Differentiates Between Sales, Closes, and Hedging

## Decision Flow

```
Trade Received
    │
    ├─→ Check: Does opposite position exist? (Up vs Down, same market)
    │   │
    │   ├─→ YES → Is it a BUY trade AND opposite_size > 0?
    │   │   │
    │   │   ├─→ YES → 🔄 HEDGING
    │   │   │   - Closes opposite position
    │   │   │   - Creates new position in current outcome
    │   │   │   - PnL = closing_size * (1.0 - entry_price - exit_price)
    │   │   │   - Event: HEDGE_CLOSE or PARTIAL_HEDGE
    │   │   │
    │   │   └─→ NO → Continue to normal position tracking
    │   │
    │   └─→ NO → Continue to normal position tracking
    │
    └─→ Check: Does same token_id position exist?
        │
        ├─→ NO → 🟢 OPEN NEW POSITION
        │   - Creates new position
        │   - Event: OPENED
        │
        └─→ YES → Check: current_size * signed_size
            │
            ├─→ > 0 (same direction) → 📈 INCREASE POSITION
            │   - Adds to existing position
            │   - Recalculates average entry price
            │   - Event: INCREASED
            │
            └─→ < 0 (opposite direction) → 🔴 CLOSE/SELL
                │
                ├─→ remaining ≈ 0 → FULL_CLOSE
                │   - Closes entire position
                │   - PnL = closing_size * (exit_price - entry_price) * direction
                │   - Event: FULL_CLOSE
                │
                ├─→ remaining flips sign → REVERSE
                │   - Closes existing position
                │   - Creates new position in opposite direction
                │   - Event: REVERSED
                │
                └─→ remaining same sign → PARTIAL_CLOSE
                    - Closes part of position
                    - Updates remaining position
                    - Event: PARTIAL_CLOSE
```

## Detailed Examples

### 1. HEDGING (Buying Opposite Outcome)
**Scenario:** Have 100 Up @ 60¢, Buy 100 Down @ 50¢

**Detection Logic:**
- ✅ Opposite position exists? YES (Up vs Down, same market)
- ✅ Is it BUY trade? YES
- ✅ Is opposite_size > 0? YES (100 Up)
- **Result:** HEDGING

**What Happens:**
- Closes 100 Up @ 60¢
- Creates 100 Down @ 50¢
- PnL = 100 * (1.0 - 0.60 - 0.50) = -$10.00
- Message: "🔄 HEDGE" or "🔄 PARTIAL HEDGE"

**Key Formula:** `PnL = closing_size * (1.0 - entry_price - exit_price)`

---

### 2. CLOSE/SELL (Selling Same Position)
**Scenario:** Have 100 Up @ 60¢, Sell 100 Up @ 70¢

**Detection Logic:**
- ❌ Opposite position exists? NO
- ✅ Same token_id position exists? YES
- ✅ current_size * signed_size < 0? YES (+100 * -100 = -10000)
- **Result:** CLOSE/SELL

**What Happens:**
- Closes 100 Up @ 60¢ → 70¢
- PnL = 100 * (0.70 - 0.60) * 1 = +$10.00
- Message: "🔴 CLOSE" or "🔴 PARTIAL CLOSE"

**Key Formula:** `PnL = closing_size * (exit_price - entry_price) * direction`

---

### 3. REVERSE (Selling More Than Owned)
**Scenario:** Have 100 Up @ 60¢, Sell 150 Up @ 70¢

**Detection Logic:**
- ❌ Opposite position exists? NO
- ✅ Same token_id position exists? YES
- ✅ current_size * signed_size < 0? YES (+100 * -150 = -15000)
- ✅ remaining flips sign? YES (100 + (-150) = -50)
- **Result:** REVERSE

**What Happens:**
- Closes 100 Up @ 60¢ → 70¢ (PnL = +$10.00)
- Creates 50 Down @ 70¢ (new short position)
- Message: "🔄 REVERSE"

---

### 4. INCREASE (Buying More of Same)
**Scenario:** Have 100 Up @ 60¢, Buy 50 Up @ 65¢

**Detection Logic:**
- ❌ Opposite position exists? NO
- ✅ Same token_id position exists? YES
- ✅ current_size * signed_size > 0? YES (+100 * +50 = +5000)
- **Result:** INCREASE

**What Happens:**
- Adds 50 Up @ 65¢ to existing 100 Up @ 60¢
- New average = (100*0.60 + 50*0.65) / 150 = $0.6167
- Total position: 150 Up @ $0.6167
- Message: "📈 +50"

---

### 5. OPEN (New Position)
**Scenario:** No position, Buy 100 Up @ 60¢

**Detection Logic:**
- ❌ Opposite position exists? NO
- ❌ Same token_id position exists? NO
- **Result:** OPEN

**What Happens:**
- Creates new position: 100 Up @ 60¢
- Message: "🟢 OPEN"

---

## Key Differences Summary

| Type | Condition | PnL Formula | Event Type |
|------|-----------|-------------|------------|
| **HEDGING** | BUY + opposite position exists | `closing_size * (1.0 - entry - exit)` | HEDGE_CLOSE / PARTIAL_HEDGE |
| **CLOSE/SELL** | SELL + same position exists | `closing_size * (exit - entry) * direction` | FULL_CLOSE / PARTIAL_CLOSE |
| **REVERSE** | SELL + flips direction | Same as CLOSE | REVERSED |
| **INCREASE** | BUY + same position exists | N/A (no PnL yet) | INCREASED |
| **OPEN** | No existing position | N/A | OPENED |

## Important Notes

1. **Hedging only happens on BUY trades** - You can't hedge by selling
2. **Hedging requires opposite outcome** - Up vs Down, same market
3. **SELL trades always close/reduce** - Never increase a position
4. **PnL is only realized on closes/hedges** - Not on opens/increases

