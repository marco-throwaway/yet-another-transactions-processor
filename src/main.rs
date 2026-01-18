use std::collections::{HashMap, hash_map::Entry};

use anyhow::{Context, Result, anyhow, bail};
use log::warn;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("error")).init();
    let input_filename = std::env::args().nth(1).context("no input file specified")?;
    let mut csv_reader = csv_reader(&input_filename)?;

    let mut ledger = Ledger::new();
    for result in csv_reader.deserialize() {
        let record = match result {
            Ok(record) => record,
            Err(e) => {
                warn!("failed to read record: {e}");
                continue;
            }
        };
        let transaction = match Transaction::try_from(&record) {
            Ok(transaction) => transaction,
            Err(e) => {
                warn!("failed to parse record: {record:?}: {e}");
                continue;
            }
        };
        if let Err(e) = process_transaction(&mut ledger, transaction) {
            warn!("failed to process transaction: {e}");
        }
    }

    let mut csv_writer = csv::Writer::from_writer(std::io::stdout());
    for (client_id, client) in ledger {
        let client_record = client.to_client_record(client_id);
        csv_writer.serialize(client_record)?;
    }

    Ok(())
}

#[derive(Debug, Deserialize, PartialEq, Eq, Hash, Clone, Copy, Serialize)]
struct ClientId(u16);

#[derive(Debug, Deserialize, PartialEq, Eq, Hash, Clone, Copy, Serialize)]
struct TransactionId(u32);

#[derive(Debug, Clone, Copy)]
enum Transaction {
    Deposit {
        client: ClientId,
        tx: TransactionId,
        amount: Decimal,
    },
    Withdrawal {
        client: ClientId,
        amount: Decimal,
    },
    Dispute {
        client: ClientId,
        tx: TransactionId,
    },
    Resolve {
        client: ClientId,
        tx: TransactionId,
    },
    Chargeback {
        client: ClientId,
        tx: TransactionId,
    },
}

impl TryFrom<&TransactionRecord> for Transaction {
    type Error = anyhow::Error;

    fn try_from(record: &TransactionRecord) -> Result<Self, Self::Error> {
        let client = record.client;
        let tx = record.tx;
        match record.tx_type {
            TransactionType::Deposit => {
                let amount = record.validated_amount()?;
                Ok(Transaction::Deposit { client, tx, amount })
            }
            TransactionType::Withdrawal => {
                let amount = record.validated_amount()?;
                Ok(Transaction::Withdrawal { client, amount })
            }
            TransactionType::Dispute => Ok(Transaction::Dispute { client, tx }),
            TransactionType::Resolve => Ok(Transaction::Resolve { client, tx }),
            TransactionType::Chargeback => Ok(Transaction::Chargeback { client, tx }),
        }
    }
}

#[derive(Debug, Deserialize)]
struct TransactionRecord {
    #[serde(rename = "type")]
    tx_type: TransactionType,
    client: ClientId,
    tx: TransactionId,
    amount: Option<Decimal>,
}

impl TransactionRecord {
    fn validated_amount(&self) -> Result<Decimal> {
        let amount = self.amount.ok_or_else(|| anyhow!("missing amount"))?;
        if amount < Decimal::ZERO {
            bail!("negative amount not allowed");
        }
        Ok(amount)
    }
}

#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

fn process_transaction(ledger: &mut Ledger, transaction: Transaction) -> Result<()> {
    match transaction {
        Transaction::Deposit { client, tx, amount } => process_deposit(ledger, client, tx, amount),
        Transaction::Withdrawal { client, amount } => process_withdrawal(ledger, client, amount),
        Transaction::Dispute { client, tx } => process_dispute(ledger, client, tx),
        Transaction::Resolve { client, tx } => process_resolve(ledger, client, tx),
        Transaction::Chargeback { client, tx } => process_chargeback(ledger, client, tx),
    }
}

fn process_deposit(
    ledger: &mut Ledger,
    client: ClientId,
    tx: TransactionId,
    amount: Decimal,
) -> Result<()> {
    let client_state = match ledger.entry(client) {
        Entry::Occupied(entry) => {
            let client_state = entry.into_mut();
            if client_state.locked {
                bail!("deposit for locked account: {client:?}");
            }
            if client_state.deposits.contains_key(&tx) {
                bail!("duplicate transaction: {tx:?}");
            }
            client_state
        }
        Entry::Vacant(entry) => entry.insert(ClientState::default()),
    };

    client_state.available += amount;
    client_state.deposits.insert(
        tx,
        StoredDeposit {
            amount,
            under_dispute: false,
        },
    );
    Ok(())
}

fn process_withdrawal(ledger: &mut Ledger, client: ClientId, amount: Decimal) -> Result<()> {
    let Some(client_state) = ledger.get_mut(&client) else {
        bail!("withdrawal for non existing account: {client:?}");
    };
    client_state.check_unlocked("withdrawal", client)?;
    if client_state.available < amount {
        bail!(
            "insufficient funds (available: {}, requested: {amount}): {client:?}",
            client_state.available
        );
    }

    client_state.available -= amount;
    Ok(())
}

fn process_dispute(ledger: &mut Ledger, client: ClientId, tx: TransactionId) -> Result<()> {
    let Some(client_state) = ledger.get_mut(&client) else {
        bail!("dispute for non existing account: {client:?}");
    };
    client_state.check_unlocked("dispute", client)?;
    let deposit = client_state.get_deposit_mut(tx, "dispute")?;
    if deposit.under_dispute {
        bail!("transaction already under dispute: {tx:?}");
    }

    let amount = deposit.amount;
    deposit.under_dispute = true;
    client_state.held += amount;
    client_state.available -= amount;
    Ok(())
}

fn process_resolve(ledger: &mut Ledger, client: ClientId, tx: TransactionId) -> Result<()> {
    let Some(client_state) = ledger.get_mut(&client) else {
        bail!("resolve for non existing account: {client:?}");
    };
    client_state.check_unlocked("resolve", client)?;
    let deposit = client_state.get_deposit_mut(tx, "resolve")?;
    if !deposit.under_dispute {
        bail!("resolve for transaction not under dispute: {tx:?}");
    }

    let amount = deposit.amount;
    deposit.under_dispute = false;
    client_state.held -= amount;
    client_state.available += amount;
    Ok(())
}

fn process_chargeback(ledger: &mut Ledger, client: ClientId, tx: TransactionId) -> Result<()> {
    let Some(client_state) = ledger.get_mut(&client) else {
        bail!("chargeback for non existing account: {client:?}");
    };
    client_state.check_unlocked("chargeback", client)?;
    let deposit = client_state.get_deposit_mut(tx, "chargeback")?;
    if !deposit.under_dispute {
        bail!("chargeback for transaction not under dispute: {tx:?}");
    }

    let amount = deposit.amount;
    deposit.under_dispute = false;
    client_state.held -= amount;
    client_state.locked = true;
    Ok(())
}

#[derive(Debug, Serialize)]
struct ClientRecord {
    client: ClientId,
    available: Decimal,
    held: Decimal,
    total: Decimal,
    locked: bool,
}

#[derive(Debug)]
struct StoredDeposit {
    amount: Decimal,
    under_dispute: bool,
}

#[derive(Debug, Default)]
struct ClientState {
    deposits: HashMap<TransactionId, StoredDeposit>,
    available: Decimal,
    held: Decimal,
    locked: bool,
}

impl ClientState {
    fn check_unlocked(&self, operation: &str, client: ClientId) -> Result<()> {
        if self.locked {
            bail!("{operation} for locked account: {client:?}");
        }
        Ok(())
    }

    fn get_deposit_mut(
        &mut self,
        tx: TransactionId,
        operation: &str,
    ) -> Result<&mut StoredDeposit> {
        self.deposits
            .get_mut(&tx)
            .ok_or_else(|| anyhow!("{operation} for non existing transaction: {tx:?}"))
    }

    fn to_client_record(&self, client: ClientId) -> ClientRecord {
        ClientRecord {
            client,
            available: self.available,
            held: self.held,
            total: self.available + self.held,
            locked: self.locked,
        }
    }
}

type Ledger = HashMap<ClientId, ClientState>;

fn csv_reader(filename: &str) -> Result<csv::Reader<Box<dyn std::io::Read>>> {
    let reader: Box<dyn std::io::Read> = if filename == "-" {
        Box::new(std::io::stdin())
    } else {
        Box::new(std::fs::File::open(filename)?)
    };

    Ok(csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_reader(reader))
}
