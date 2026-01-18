# Yet Another Transactions Processor

## Usage

```bash
cargo run -- transactions.csv > accounts.csv

# Or read from stdin:
cat transactions.csv | cargo run -- - > accounts.csv

# Run with warnings enabled (default is errors only):
RUST_LOG=warn cargo run -- transactions.csv > accounts.csv
```

## Tests

```bash
cargo test
```

## Assumptions

Here are some assumptions I made that weren't explicitly stated in the spec:

- The CSV file always has a header row
- We don't stop processing on errors (for example csv format errors, unknown transaction types or transaction errors), instead we just skip and log warnings.
- New accounts are only created on deposits, other transactions are assumed to be mistakes and ignored.
- All transactions are ignored on locked accounts including further chargebacks.
- A dispute can cause negative available balance if the client already withdrew some of the disputed funds.
- Disputes only happen on deposits, not withdrawals.
- A transaction can be disputed again after being resolved (but not while already under dispute).
- Dispute/resolve/chargeback must reference a transaction belonging to the client.

## AI usage

I decided to write this myself as it's been a fun challenge and will make it easier for me to explain the code later on.

Exceptions:

- Used copilot autocomplete in neovim for boilerplate code.
- Used claude to write the integration tests. Most passed on first try but this helped me fix some special cases.
- Used claude to refactor errors to `thiserror` from `anyhow` but then decided to undo it again as it seemed overkill.
