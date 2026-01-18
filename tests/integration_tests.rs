use std::io::Write;
use std::process::{Command, Stdio};

use rust_decimal::Decimal;
use serde::Deserialize;

/// Represents a client record from the output CSV.
#[derive(Debug, Clone, Deserialize, PartialEq)]
struct ClientRecord {
    client: u16,
    available: Decimal,
    held: Decimal,
    total: Decimal,
    locked: bool,
}

const BIN_PATH: &str = env!("CARGO_BIN_EXE_yet-another-transactions-processor");

/// Runs the payments engine with the given input CSV via STDIN and returns parsed output.
fn run_engine(input: &str) -> Vec<ClientRecord> {
    let mut child = Command::new(BIN_PATH)
        .arg("-")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to start payments engine");

    child
        .stdin
        .take()
        .expect("Failed to open stdin")
        .write_all(input.as_bytes())
        .expect("Failed to write to stdin");

    let output = child.wait_with_output().expect("Failed to read stdout");

    assert!(
        output.status.success(),
        "Process failed with {}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("Invalid UTF-8");
    parse_output(&stdout)
}

/// Runs the payments engine with input from a file and returns parsed output.
fn run_engine_from_file(path: impl AsRef<std::path::Path>) -> Vec<ClientRecord> {
    let output = Command::new("cargo")
        .args(["run", "--quiet", "--"])
        .arg(path.as_ref())
        .output()
        .expect("Failed to run cargo");

    assert!(
        output.status.success(),
        "Process failed with {}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("Invalid UTF-8");
    parse_output(&stdout)
}

/// Parses the CSV output into a vector of ``ClientRecords``.
fn parse_output(output: &str) -> Vec<ClientRecord> {
    let mut reader = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_reader(output.as_bytes());

    reader
        .deserialize()
        .map(|r| r.unwrap_or_else(|e| panic!("Failed to parse record: {e}\nRaw output:\n{output}")))
        .collect()
}

/// Asserts that two sets of client records are equivalent (order-independent).
fn assert_records_eq(mut actual: Vec<ClientRecord>, mut expected: Vec<ClientRecord>) {
    actual.sort_by_key(|r| r.client);
    expected.sort_by_key(|r| r.client);
    assert_eq!(actual, expected);
}

/// Parses a string into a Decimal for test assertions.
fn dec(s: &str) -> Decimal {
    s.parse().unwrap()
}

// =============================================================================
// 1. Basic Deposit Tests
// =============================================================================

mod deposit {
    use super::*;

    /// A single deposit creates a client and credits the account.
    #[test]
    fn single_deposit_creates_client() {
        let input = "\
type,client,tx,amount
deposit,1,1,100.0";

        let actual = run_engine(input);
        let expected = vec![ClientRecord {
            client: 1,
            available: dec("100.0"),
            held: dec("0.0"),
            total: dec("100.0"),
            locked: false,
        }];

        assert_records_eq(actual, expected);
    }

    /// Multiple deposits to the same client accumulate in available and total.
    #[test]
    fn multiple_deposits_same_client() {
        let input = "\
type,client,tx,amount
deposit,1,1,50.0
deposit,1,2,25.5
deposit,1,3,24.5";

        let actual = run_engine(input);
        let expected = vec![ClientRecord {
            client: 1,
            available: dec("100.0"),
            held: dec("0.0"),
            total: dec("100.0"),
            locked: false,
        }];

        assert_records_eq(actual, expected);
    }

    /// Deposits handle up to 4 decimal places of precision.
    #[test]
    fn max_precision_four_decimals() {
        let input = "\
type,client,tx,amount
deposit,1,1,1.2345
deposit,1,2,0.0001";

        let actual = run_engine(input);
        let expected = vec![ClientRecord {
            client: 1,
            available: dec("1.2346"),
            held: dec("0.0"),
            total: dec("1.2346"),
            locked: false,
        }];

        assert_records_eq(actual, expected);
    }

    /// A deposit of zero is valid but doesn't change balances.
    #[test]
    fn zero_amount_deposit() {
        let input = "\
type,client,tx,amount
deposit,1,1,100.0
deposit,1,2,0.0";

        let actual = run_engine(input);
        let expected = vec![ClientRecord {
            client: 1,
            available: dec("100.0"),
            held: dec("0.0"),
            total: dec("100.0"),
            locked: false,
        }];

        assert_records_eq(actual, expected);
    }
}

// =============================================================================
// 2. Basic Withdrawal Tests
// =============================================================================

mod withdrawal {
    use super::*;

    /// Withdrawal with sufficient funds decreases available and total.
    #[test]
    fn successful_withdrawal() {
        let input = "\
type,client,tx,amount
deposit,1,1,100.0
withdrawal,1,2,40.0";

        let actual = run_engine(input);
        let expected = vec![ClientRecord {
            client: 1,
            available: dec("60.0"),
            held: dec("0.0"),
            total: dec("60.0"),
            locked: false,
        }];

        assert_records_eq(actual, expected);
    }

    /// Withdrawing the exact available balance leaves zero.
    #[test]
    fn withdraw_exact_balance() {
        let input = "\
type,client,tx,amount
deposit,1,1,50.0
withdrawal,1,2,50.0";

        let actual = run_engine(input);
        let expected = vec![ClientRecord {
            client: 1,
            available: dec("0.0"),
            held: dec("0.0"),
            total: dec("0.0"),
            locked: false,
        }];

        assert_records_eq(actual, expected);
    }

    /// Withdrawal exceeding available funds fails silently; balances unchanged.
    #[test]
    fn insufficient_funds() {
        let input = "\
type,client,tx,amount
deposit,1,1,50.0
withdrawal,1,2,100.0";

        let actual = run_engine(input);
        let expected = vec![ClientRecord {
            client: 1,
            available: dec("50.0"),
            held: dec("0.0"),
            total: dec("50.0"),
            locked: false,
        }];

        assert_records_eq(actual, expected);
    }

    /// Multiple sequential withdrawals each deduct from available funds.
    #[test]
    fn multiple_withdrawals() {
        let input = "\
type,client,tx,amount
deposit,1,1,100.0
withdrawal,1,2,30.0
withdrawal,1,3,20.0
withdrawal,1,4,10.0";

        let actual = run_engine(input);
        let expected = vec![ClientRecord {
            client: 1,
            available: dec("40.0"),
            held: dec("0.0"),
            total: dec("40.0"),
            locked: false,
        }];

        assert_records_eq(actual, expected);
    }

    /// Failed withdrawal mid-sequence doesn't prevent subsequent valid withdrawals.
    #[test]
    fn failed_withdrawal_mid_sequence() {
        let input = "\
type,client,tx,amount
deposit,1,1,100.0
withdrawal,1,2,60.0
withdrawal,1,3,50.0
withdrawal,1,4,30.0";

        let actual = run_engine(input);
        // After deposit: 100
        // After w1 (60): 40
        // w2 (50) fails (insufficient)
        // After w3 (30): 10
        let expected = vec![ClientRecord {
            client: 1,
            available: dec("10.0"),
            held: dec("0.0"),
            total: dec("10.0"),
            locked: false,
        }];

        assert_records_eq(actual, expected);
    }
}

// =============================================================================
// 3. Dispute Tests
// =============================================================================

mod dispute {
    use super::*;

    /// Disputing a deposit moves funds from available to held.
    #[test]
    fn basic_dispute() {
        let input = "\
type,client,tx,amount
deposit,1,1,100.0
dispute,1,1,";

        let actual = run_engine(input);
        let expected = vec![ClientRecord {
            client: 1,
            available: dec("0.0"),
            held: dec("100.0"),
            total: dec("100.0"),
            locked: false,
        }];

        assert_records_eq(actual, expected);
    }

    /// Disputing one of multiple deposits only holds that amount.
    #[test]
    fn partial_funds_under_dispute() {
        let input = "\
type,client,tx,amount
deposit,1,1,100.0
deposit,1,2,50.0
dispute,1,1,";

        let actual = run_engine(input);
        let expected = vec![ClientRecord {
            client: 1,
            available: dec("50.0"),
            held: dec("100.0"),
            total: dec("150.0"),
            locked: false,
        }];

        assert_records_eq(actual, expected);
    }

    /// Disputing a non-existent transaction is ignored.
    #[test]
    fn nonexistent_transaction() {
        let input = "\
type,client,tx,amount
deposit,1,1,100.0
dispute,1,999,";

        let actual = run_engine(input);
        let expected = vec![ClientRecord {
            client: 1,
            available: dec("100.0"),
            held: dec("0.0"),
            total: dec("100.0"),
            locked: false,
        }];

        assert_records_eq(actual, expected);
    }

    /// Dispute ignores any amount field and uses the referenced TX amount.
    #[test]
    fn amount_field_ignored() {
        let input = "\
type,client,tx,amount
deposit,1,1,100.0
dispute,1,1,999.0";

        let actual = run_engine(input);
        let expected = vec![ClientRecord {
            client: 1,
            available: dec("0.0"),
            held: dec("100.0"),
            total: dec("100.0"),
            locked: false,
        }];

        assert_records_eq(actual, expected);
    }

    /// Multiple transactions can be disputed simultaneously.
    #[test]
    fn multiple_disputes() {
        let input = "\
type,client,tx,amount
deposit,1,1,100.0
deposit,1,2,50.0
deposit,1,3,25.0
dispute,1,1,
dispute,1,2,";

        let actual = run_engine(input);
        let expected = vec![ClientRecord {
            client: 1,
            available: dec("25.0"),
            held: dec("150.0"),
            total: dec("175.0"),
            locked: false,
        }];

        assert_records_eq(actual, expected);
    }
}

// =============================================================================
// 4. Resolve Tests
// =============================================================================

mod resolve {
    use super::*;

    /// Resolving a dispute releases held funds back to available.
    #[test]
    fn basic_resolve() {
        let input = "\
type,client,tx,amount
deposit,1,1,100.0
dispute,1,1,
resolve,1,1,";

        let actual = run_engine(input);
        let expected = vec![ClientRecord {
            client: 1,
            available: dec("100.0"),
            held: dec("0.0"),
            total: dec("100.0"),
            locked: false,
        }];

        assert_records_eq(actual, expected);
    }

    /// Resolving a non-existent transaction is ignored.
    #[test]
    fn nonexistent_transaction() {
        let input = "\
type,client,tx,amount
deposit,1,1,100.0
dispute,1,1,
resolve,1,999,";

        let actual = run_engine(input);
        let expected = vec![ClientRecord {
            client: 1,
            available: dec("0.0"),
            held: dec("100.0"),
            total: dec("100.0"),
            locked: false,
        }];

        assert_records_eq(actual, expected);
    }

    /// Resolving a transaction not under dispute is ignored.
    #[test]
    fn not_under_dispute() {
        let input = "\
type,client,tx,amount
deposit,1,1,100.0
deposit,1,2,50.0
dispute,1,1,
resolve,1,2,";

        let actual = run_engine(input);
        let expected = vec![ClientRecord {
            client: 1,
            available: dec("50.0"),
            held: dec("100.0"),
            total: dec("150.0"),
            locked: false,
        }];

        assert_records_eq(actual, expected);
    }

    /// Resolving one of multiple disputes only affects that transaction.
    #[test]
    fn partial_resolve() {
        let input = "\
type,client,tx,amount
deposit,1,1,100.0
deposit,1,2,50.0
dispute,1,1,
dispute,1,2,
resolve,1,1,";

        let actual = run_engine(input);
        let expected = vec![ClientRecord {
            client: 1,
            available: dec("100.0"),
            held: dec("50.0"),
            total: dec("150.0"),
            locked: false,
        }];

        assert_records_eq(actual, expected);
    }
}

// =============================================================================
// 5. Chargeback Tests
// =============================================================================

mod chargeback {
    use super::*;

    /// Chargeback removes held funds and locks the account.
    #[test]
    fn basic_chargeback() {
        let input = "\
type,client,tx,amount
deposit,1,1,100.0
dispute,1,1,
chargeback,1,1,";

        let actual = run_engine(input);
        let expected = vec![ClientRecord {
            client: 1,
            available: dec("0.0"),
            held: dec("0.0"),
            total: dec("0.0"),
            locked: true,
        }];

        assert_records_eq(actual, expected);
    }

    /// Chargeback on one of multiple deposits only removes that amount.
    #[test]
    fn partial_chargeback() {
        let input = "\
type,client,tx,amount
deposit,1,1,100.0
deposit,1,2,50.0
dispute,1,1,
chargeback,1,1,";

        let actual = run_engine(input);
        let expected = vec![ClientRecord {
            client: 1,
            available: dec("50.0"),
            held: dec("0.0"),
            total: dec("50.0"),
            locked: true,
        }];

        assert_records_eq(actual, expected);
    }

    /// Chargeback on non-existent transaction is ignored.
    #[test]
    fn nonexistent_transaction() {
        let input = "\
type,client,tx,amount
deposit,1,1,100.0
dispute,1,1,
chargeback,1,999,";

        let actual = run_engine(input);
        let expected = vec![ClientRecord {
            client: 1,
            available: dec("0.0"),
            held: dec("100.0"),
            total: dec("100.0"),
            locked: false,
        }];

        assert_records_eq(actual, expected);
    }

    /// Chargeback on transaction not under dispute is ignored.
    #[test]
    fn not_under_dispute() {
        let input = "\
type,client,tx,amount
deposit,1,1,100.0
deposit,1,2,50.0
dispute,1,1,
chargeback,1,2,";

        let actual = run_engine(input);
        let expected = vec![ClientRecord {
            client: 1,
            available: dec("50.0"),
            held: dec("100.0"),
            total: dec("150.0"),
            locked: false,
        }];

        assert_records_eq(actual, expected);
    }

    /// Chargeback affects only the specified disputed transaction.
    #[test]
    fn one_of_multiple_disputes() {
        let input = "\
type,client,tx,amount
deposit,1,1,100.0
deposit,1,2,50.0
dispute,1,1,
dispute,1,2,
chargeback,1,1,";

        let actual = run_engine(input);
        let expected = vec![ClientRecord {
            client: 1,
            available: dec("0.0"),
            held: dec("50.0"),
            total: dec("50.0"),
            locked: true,
        }];

        assert_records_eq(actual, expected);
    }
}

// =============================================================================
// 6. Multi-Client Tests
// =============================================================================

mod multi_client {
    use super::*;

    /// Multiple clients have independent accounts.
    #[test]
    fn independent_accounts() {
        let input = "\
type,client,tx,amount
deposit,1,1,100.0
deposit,2,2,200.0
deposit,3,3,300.0";

        let actual = run_engine(input);
        let expected = vec![
            ClientRecord {
                client: 1,
                available: dec("100.0"),
                held: dec("0.0"),
                total: dec("100.0"),
                locked: false,
            },
            ClientRecord {
                client: 2,
                available: dec("200.0"),
                held: dec("0.0"),
                total: dec("200.0"),
                locked: false,
            },
            ClientRecord {
                client: 3,
                available: dec("300.0"),
                held: dec("0.0"),
                total: dec("300.0"),
                locked: false,
            },
        ];

        assert_records_eq(actual, expected);
    }

    /// Client IDs don't need to appear in order.
    #[test]
    fn non_sequential_client_ids() {
        let input = "\
type,client,tx,amount
deposit,5,1,500.0
deposit,1,2,100.0
deposit,3,3,300.0";

        let actual = run_engine(input);
        let expected = vec![
            ClientRecord {
                client: 1,
                available: dec("100.0"),
                held: dec("0.0"),
                total: dec("100.0"),
                locked: false,
            },
            ClientRecord {
                client: 3,
                available: dec("300.0"),
                held: dec("0.0"),
                total: dec("300.0"),
                locked: false,
            },
            ClientRecord {
                client: 5,
                available: dec("500.0"),
                held: dec("0.0"),
                total: dec("500.0"),
                locked: false,
            },
        ];

        assert_records_eq(actual, expected);
    }

    /// A dispute on one client's transaction doesn't affect other clients.
    #[test]
    fn dispute_isolation() {
        let input = "\
type,client,tx,amount
deposit,1,1,100.0
deposit,2,2,100.0
dispute,1,1,";

        let actual = run_engine(input);
        let expected = vec![
            ClientRecord {
                client: 1,
                available: dec("0.0"),
                held: dec("100.0"),
                total: dec("100.0"),
                locked: false,
            },
            ClientRecord {
                client: 2,
                available: dec("100.0"),
                held: dec("0.0"),
                total: dec("100.0"),
                locked: false,
            },
        ];

        assert_records_eq(actual, expected);
    }

    /// Locking one client's account doesn't affect other clients.
    #[test]
    fn chargeback_isolation() {
        let input = "\
type,client,tx,amount
deposit,1,1,100.0
deposit,2,2,100.0
dispute,1,1,
chargeback,1,1,";

        let actual = run_engine(input);
        let expected = vec![
            ClientRecord {
                client: 1,
                available: dec("0.0"),
                held: dec("0.0"),
                total: dec("0.0"),
                locked: true,
            },
            ClientRecord {
                client: 2,
                available: dec("100.0"),
                held: dec("0.0"),
                total: dec("100.0"),
                locked: false,
            },
        ];

        assert_records_eq(actual, expected);
    }

    /// The example from the specification document.
    #[test]
    fn spec_example() {
        let input = "\
type,client,tx,amount
deposit,1,1,1.0
deposit,2,2,2.0
deposit,1,3,2.0
withdrawal,1,4,1.5
withdrawal,2,5,3.0";

        let actual = run_engine(input);
        let expected = vec![
            ClientRecord {
                client: 1,
                available: dec("1.5"),
                held: dec("0.0"),
                total: dec("1.5"),
                locked: false,
            },
            ClientRecord {
                client: 2,
                available: dec("2.0"),
                held: dec("0.0"),
                total: dec("2.0"),
                locked: false,
            },
        ];

        assert_records_eq(actual, expected);
    }
}

// =============================================================================
// 7. Precision Tests
// =============================================================================

mod precision {
    use super::*;

    /// Operations maintain 4 decimal place precision.
    #[test]
    fn four_decimal_precision() {
        let input = "\
type,client,tx,amount
deposit,1,1,0.0001
deposit,1,2,0.0002
withdrawal,1,3,0.0001";

        let actual = run_engine(input);
        let expected = vec![ClientRecord {
            client: 1,
            available: dec("0.0002"),
            held: dec("0.0"),
            total: dec("0.0002"),
            locked: false,
        }];

        assert_records_eq(actual, expected);
    }

    /// Large numbers maintain precision.
    #[test]
    fn large_numbers() {
        let input = "\
type,client,tx,amount
deposit,1,1,999999.9999
deposit,1,2,0.0001";

        let actual = run_engine(input);
        let expected = vec![ClientRecord {
            client: 1,
            available: dec("1000000.0"),
            held: dec("0.0"),
            total: dec("1000000.0"),
            locked: false,
        }];

        assert_records_eq(actual, expected);
    }

    /// Withdrawal works correctly at precision boundary.
    #[test]
    fn precision_withdrawal() {
        let input = "\
type,client,tx,amount
deposit,1,1,100.0000
withdrawal,1,2,99.9999";

        let actual = run_engine(input);
        let expected = vec![ClientRecord {
            client: 1,
            available: dec("0.0001"),
            held: dec("0.0"),
            total: dec("0.0001"),
            locked: false,
        }];

        assert_records_eq(actual, expected);
    }
}

// =============================================================================
// 8. Whitespace Handling Tests
// =============================================================================

mod whitespace {
    use super::*;

    /// CSV with spaces after commas is parsed correctly.
    #[test]
    fn spaces_after_commas() {
        let input = "\
type, client, tx, amount
deposit, 1, 1, 100.0
withdrawal, 1, 2, 50.0";

        let actual = run_engine(input);
        let expected = vec![ClientRecord {
            client: 1,
            available: dec("50.0"),
            held: dec("0.0"),
            total: dec("50.0"),
            locked: false,
        }];

        assert_records_eq(actual, expected);
    }

    /// Mixed whitespace formatting is handled.
    #[test]
    fn inconsistent_whitespace() {
        let input = "\
type,client,tx,amount
deposit,1,1,100.0
deposit, 1, 2, 50.0
withdrawal,1,3,25.0";

        let actual = run_engine(input);
        let expected = vec![ClientRecord {
            client: 1,
            available: dec("125.0"),
            held: dec("0.0"),
            total: dec("125.0"),
            locked: false,
        }];

        assert_records_eq(actual, expected);
    }

    /// Values with surrounding whitespace are trimmed.
    #[test]
    fn leading_trailing_spaces() {
        let input = "\
type,client,tx,amount
deposit, 1 , 1 , 100.0";

        let actual = run_engine(input);
        let expected = vec![ClientRecord {
            client: 1,
            available: dec("100.0"),
            held: dec("0.0"),
            total: dec("100.0"),
            locked: false,
        }];

        assert_records_eq(actual, expected);
    }
}

// =============================================================================
// 9. Error Handling / Edge Case Tests
// =============================================================================

mod error_handling {
    use super::*;

    /// Empty input file (header only) produces empty output.
    #[test]
    fn empty_file() {
        let input = "type,client,tx,amount";

        let actual = run_engine(input);
        assert!(actual.is_empty());
    }

    /// Chargeback without prior dispute should be ignored.
    #[test]
    fn chargeback_without_dispute() {
        let input = "\
type,client,tx,amount
deposit,1,1,100.0
chargeback,1,1,";

        let actual = run_engine(input);
        let expected = vec![ClientRecord {
            client: 1,
            available: dec("100.0"),
            held: dec("0.0"),
            total: dec("100.0"),
            locked: false,
        }];

        assert_records_eq(actual, expected);
    }

    /// Resolve without prior dispute should be ignored.
    #[test]
    fn resolve_without_dispute() {
        let input = "\
type,client,tx,amount
deposit,1,1,100.0
resolve,1,1,";

        let actual = run_engine(input);
        let expected = vec![ClientRecord {
            client: 1,
            available: dec("100.0"),
            held: dec("0.0"),
            total: dec("100.0"),
            locked: false,
        }];

        assert_records_eq(actual, expected);
    }

    /// Transaction IDs can appear in any order.
    /// Spec: "transaction IDs (tx) are globally unique, though are also not guaranteed to be ordered"
    #[test]
    fn tx_ids_out_of_order() {
        let input = "\
type,client,tx,amount
deposit,1,100,50.0
deposit,1,5,30.0
deposit,1,50,20.0";

        let actual = run_engine(input);
        let expected = vec![ClientRecord {
            client: 1,
            available: dec("100.0"),
            held: dec("0.0"),
            total: dec("100.0"),
            locked: false,
        }];

        assert_records_eq(actual, expected);
    }
}

// =============================================================================
// 10. Complex Scenario Tests
// =============================================================================

mod complex_scenarios {
    use super::*;

    /// Complete dispute resolution flow: Deposit -> Dispute -> Resolve.
    #[test]
    fn full_dispute_resolve_lifecycle() {
        let input = "\
type,client,tx,amount
deposit,1,1,100.0
deposit,1,2,50.0
dispute,1,1,
resolve,1,1,";

        let actual = run_engine(input);
        let expected = vec![ClientRecord {
            client: 1,
            available: dec("150.0"),
            held: dec("0.0"),
            total: dec("150.0"),
            locked: false,
        }];

        assert_records_eq(actual, expected);
    }

    /// Complete chargeback flow: Deposit -> Dispute -> Chargeback.
    #[test]
    fn full_chargeback_lifecycle() {
        let input = "\
type,client,tx,amount
deposit,1,1,100.0
deposit,1,2,50.0
dispute,1,1,
chargeback,1,1,";

        let actual = run_engine(input);
        let expected = vec![ClientRecord {
            client: 1,
            available: dec("50.0"),
            held: dec("0.0"),
            total: dec("50.0"),
            locked: true,
        }];

        assert_records_eq(actual, expected);
    }

    /// Complex interleaving of operations across clients.
    #[test]
    fn interleaved_operations() {
        let input = "\
type,client,tx,amount
deposit,1,1,100.0
deposit,2,2,200.0
withdrawal,1,3,50.0
dispute,2,2,
deposit,1,4,25.0
resolve,2,2,
withdrawal,2,5,100.0";

        let actual = run_engine(input);
        let expected = vec![
            ClientRecord {
                client: 1,
                available: dec("75.0"),
                held: dec("0.0"),
                total: dec("75.0"),
                locked: false,
            },
            ClientRecord {
                client: 2,
                available: dec("100.0"),
                held: dec("0.0"),
                total: dec("100.0"),
                locked: false,
            },
        ];

        assert_records_eq(actual, expected);
    }

    /// Multiple disputes with only some resolved.
    #[test]
    fn partial_resolutions() {
        let input = "\
type,client,tx,amount
deposit,1,1,100.0
deposit,1,2,50.0
deposit,1,3,25.0
dispute,1,1,
dispute,1,2,
dispute,1,3,
resolve,1,2,";

        let actual = run_engine(input);
        let expected = vec![ClientRecord {
            client: 1,
            available: dec("50.0"),
            held: dec("125.0"),
            total: dec("175.0"),
            locked: false,
        }];

        assert_records_eq(actual, expected);
    }
}

// =============================================================================
// 11. Spec-Based Constraint Tests
// =============================================================================

mod spec_constraints {
    use super::*;

    /// System handles maximum client ID (u16 max = 65535).
    /// Spec: "the client column is a valid u16 client ID"
    #[test]
    fn max_client_id() {
        let input = "\
type,client,tx,amount
deposit,65535,1,100.0";

        let actual = run_engine(input);
        let expected = vec![ClientRecord {
            client: 65535,
            available: dec("100.0"),
            held: dec("0.0"),
            total: dec("100.0"),
            locked: false,
        }];

        assert_records_eq(actual, expected);
    }

    /// System handles maximum transaction ID (u32 max = 4294967295).
    /// Spec: "the tx is a valid u32 transaction ID"
    #[test]
    fn max_tx_id() {
        let input = "\
type,client,tx,amount
deposit,1,4294967295,100.0";

        let actual = run_engine(input);
        let expected = vec![ClientRecord {
            client: 1,
            available: dec("100.0"),
            held: dec("0.0"),
            total: dec("100.0"),
            locked: false,
        }];

        assert_records_eq(actual, expected);
    }

    /// Total always equals available + held (invariant check).
    /// Spec: "total... should be equal to available + held"
    #[test]
    fn total_invariant() {
        let input = "\
type,client,tx,amount
deposit,1,1,100.0
deposit,1,2,50.0
withdrawal,1,3,30.0
dispute,1,1,
deposit,1,4,20.0";

        let actual = run_engine(input);
        let expected = vec![ClientRecord {
            client: 1,
            available: dec("40.0"),
            held: dec("100.0"),
            total: dec("140.0"),
            locked: false,
        }];

        // Verify invariant: available + held = total
        let record = &actual[0];
        assert_eq!(record.available + record.held, record.total);

        assert_records_eq(actual, expected);
    }
}

// =============================================================================
// File-based Test (from specification PDF example)
// =============================================================================

mod file_based {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    /// Test reading from an actual file (the example from the specification PDF).
    #[test]
    fn spec_example_from_file() {
        let input_content = "\
type,client,tx,amount
deposit,1,1,1.0
deposit,2,2,2.0
deposit,1,3,2.0
withdrawal,1,4,1.5
withdrawal,2,5,3.0";

        let mut file = NamedTempFile::new().expect("Failed to create temp file");
        file.write_all(input_content.as_bytes())
            .expect("Failed to write temp file");
        file.flush().expect("Failed to flush temp file");

        let actual = run_engine_from_file(file.path());
        let expected = vec![
            ClientRecord {
                client: 1,
                available: dec("1.5"),
                held: dec("0.0"),
                total: dec("1.5"),
                locked: false,
            },
            ClientRecord {
                client: 2,
                available: dec("2.0"),
                held: dec("0.0"),
                total: dec("2.0"),
                locked: false,
            },
        ];

        assert_records_eq(actual, expected);
    }
}
