//! This crate is used for emitting blockchain data from the `bitcoind` RPC interface. It does not
//! use the wallet RPC API, so this crate can be used with wallet-disabled Bitcoin Core nodes.
//!
//! [`Emitter`] is the main structure which sources blockchain data from [`bitcoincore_rpc::Client`].
//!
//! To only get block updates (exclude mempool transactions), the caller can use
//! [`Emitter::next_block`] or/and [`Emitter::next_header`] until it returns `Ok(None)` (which means
//! the chain tip is reached). A separate method, [`Emitter::mempool`] can be used to emit the whole
//! mempool.
#![warn(missing_docs)]

use bdk_chain::{local_chain::CheckPoint, BlockId};
use bitcoin::{block::Header, Block, BlockHash, Transaction};
pub use bitcoincore_rpc;
use bitcoincore_rpc::bitcoincore_rpc_json;

/// A structure that emits data sourced from [`bitcoincore_rpc::Client`].
///
/// Refer to [module-level documentation] for more.
///
/// [module-level documentation]: crate
pub struct Emitter<'c, C> {
    client: &'c C,
    start_height: u32,

    /// The checkpoint of the last-emitted block that is in the best chain. If it is later found
    /// that the block is no longer in the best chain, it will be popped off from here.
    last_cp: Option<CheckPoint>,

    /// The block result returned from rpc of the last-emitted block. As this result contains the
    /// next block's block hash (which we use to fetch the next block), we set this to `None`
    /// whenever there are no more blocks, or the next block is no longer in the best chain. This
    /// gives us an opportunity to re-fetch this result.
    last_block: Option<bitcoincore_rpc_json::GetBlockResult>,

    /// The latest first-seen epoch of emitted mempool transactions. This is used to determine
    /// whether a mempool transaction is already emitted.
    last_mempool_time: usize,

    /// The last emitted block during our last mempool emission. This is used to determine whether
    /// there has been a reorg since our last mempool emission.
    last_mempool_tip: Option<u32>,
}

impl<'c, C: bitcoincore_rpc::RpcApi> Emitter<'c, C> {
    /// Construct a new [`Emitter`] with the given RPC `client` and `start_height`.
    ///
    /// `start_height` is the block height to start emitting blocks from.
    pub fn from_height(client: &'c C, start_height: u32) -> Self {
        Self {
            client,
            start_height,
            last_cp: None,
            last_block: None,
            last_mempool_time: 0,
            last_mempool_tip: None,
        }
    }

    /// Construct a new [`Emitter`] with the given RPC `client` and `checkpoint`.
    ///
    /// `checkpoint` is used to find the latest block which is still part of the best chain. The
    /// [`Emitter`] will emit blocks starting right above this block.
    pub fn from_checkpoint(client: &'c C, checkpoint: CheckPoint) -> Self {
        Self {
            client,
            start_height: 0,
            last_cp: Some(checkpoint),
            last_block: None,
            last_mempool_time: 0,
            last_mempool_tip: None,
        }
    }

    /// Emit mempool transactions, alongside their first-seen unix timestamps.
    ///
    /// This method emits each transaction only once, unless we cannot guarantee the transaction's
    /// ancestors are already emitted.
    ///
    /// To understand why, consider a receiver which filters transactions based on whether it
    /// alters the UTXO set of tracked script pubkeys. If an emitted mempool transaction spends a
    /// tracked UTXO which is confirmed at height `h`, but the receiver has only seen up to block
    /// of height `h-1`, we want to re-emit this transaction until the receiver has seen the block
    /// at height `h`.
    pub fn mempool(&mut self) -> Result<Vec<(Transaction, u64)>, bitcoincore_rpc::Error> {
        let client = self.client;

        // This is the emitted tip height during the last mempool emission.
        let prev_mempool_tip = self
            .last_mempool_tip
            // We use `start_height - 1` as we cannot guarantee that the block at
            // `start_height` has been emitted.
            .unwrap_or(self.start_height.saturating_sub(1));

        // Mempool txs come with a timestamp of when the tx is introduced to the mempool. We keep
        // track of the latest mempool tx's timestamp to determine whether we have seen a tx
        // before. `prev_mempool_time` is the previous timestamp and `last_time` records what will
        // be the new latest timestamp.
        let prev_mempool_time = self.last_mempool_time;
        let mut latest_time = prev_mempool_time;

        let txs_to_emit = client
            .get_raw_mempool_verbose()?
            .into_iter()
            .filter_map({
                let latest_time = &mut latest_time;
                move |(txid, tx_entry)| -> Option<Result<_, bitcoincore_rpc::Error>> {
                    let tx_time = tx_entry.time as usize;
                    if tx_time > *latest_time {
                        *latest_time = tx_time;
                    }

                    // Avoid emitting transactions that are already emitted if we can guarantee
                    // blocks containing ancestors are already emitted. The bitcoind rpc interface
                    // provides us with the block height that the tx is introduced to the mempool.
                    // If we have already emitted the block of height, we can assume that all
                    // ancestor txs have been processed by the receiver.
                    let is_already_emitted = tx_time <= prev_mempool_time;
                    let is_within_height = tx_entry.height <= prev_mempool_tip as _;
                    if is_already_emitted && is_within_height {
                        return None;
                    }

                    let tx = match client.get_raw_transaction(&txid, None) {
                        Ok(tx) => tx,
                        // the tx is confirmed or evicted since `get_raw_mempool_verbose`
                        Err(err) if err.is_not_found_error() => return None,
                        Err(err) => return Some(Err(err)),
                    };

                    Some(Ok((tx, tx_time as u64)))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        self.last_mempool_time = latest_time;
        self.last_mempool_tip = self.last_cp.as_ref().map(|cp| cp.height());

        Ok(txs_to_emit)
    }

    /// Emit the next block height and header (if any).
    pub fn next_header(&mut self) -> Result<Option<(u32, Header)>, bitcoincore_rpc::Error> {
        poll(self, |hash| self.client.get_block_header(hash))
    }

    /// Emit the next block height and block (if any).
    pub fn next_block(&mut self) -> Result<Option<(u32, Block)>, bitcoincore_rpc::Error> {
        poll(self, |hash| self.client.get_block(hash))
    }
}

enum PollResponse {
    Block(bitcoincore_rpc_json::GetBlockResult),
    NoMoreBlocks,
    /// Fetched block is not in the best chain.
    BlockNotInBestChain,
    AgreementFound(bitcoincore_rpc_json::GetBlockResult, CheckPoint),
    AgreementPointNotFound,
}

fn poll_once<C>(emitter: &Emitter<C>) -> Result<PollResponse, bitcoincore_rpc::Error>
where
    C: bitcoincore_rpc::RpcApi,
{
    let client = emitter.client;

    if let Some(last_res) = &emitter.last_block {
        assert!(
            emitter.last_cp.is_some(),
            "must not have block result without last cp"
        );

        let next_hash = match last_res.nextblockhash {
            None => return Ok(PollResponse::NoMoreBlocks),
            Some(next_hash) => next_hash,
        };

        let res = client.get_block_info(&next_hash)?;
        if res.confirmations < 0 {
            return Ok(PollResponse::BlockNotInBestChain);
        }
        return Ok(PollResponse::Block(res));
    }

    if emitter.last_cp.is_none() {
        let hash = client.get_block_hash(emitter.start_height as _)?;

        let res = client.get_block_info(&hash)?;
        if res.confirmations < 0 {
            return Ok(PollResponse::BlockNotInBestChain);
        }
        return Ok(PollResponse::Block(res));
    }

    for cp in emitter.last_cp.iter().flat_map(CheckPoint::iter) {
        let res = client.get_block_info(&cp.hash())?;
        if res.confirmations < 0 {
            // block is not in best chain
            continue;
        }

        // agreement point found
        return Ok(PollResponse::AgreementFound(res, cp));
    }

    Ok(PollResponse::AgreementPointNotFound)
}

fn poll<C, V, F>(
    emitter: &mut Emitter<C>,
    get_item: F,
) -> Result<Option<(u32, V)>, bitcoincore_rpc::Error>
where
    C: bitcoincore_rpc::RpcApi,
    F: Fn(&BlockHash) -> Result<V, bitcoincore_rpc::Error>,
{
    loop {
        match poll_once(emitter)? {
            PollResponse::Block(res) => {
                let height = res.height as u32;
                let hash = res.hash;
                let item = get_item(&hash)?;

                let this_id = BlockId { height, hash };
                let prev_id = res.previousblockhash.map(|prev_hash| BlockId {
                    height: height - 1,
                    hash: prev_hash,
                });

                match (&mut emitter.last_cp, prev_id) {
                    (Some(cp), _) => *cp = cp.clone().push(this_id).expect("must push"),
                    (last_cp, None) => *last_cp = Some(CheckPoint::new(this_id)),
                    // When the receiver constructs a local_chain update from a block, the previous
                    // checkpoint is also included in the update. We need to reflect this state in
                    // `Emitter::last_cp` as well.
                    (last_cp, Some(prev_id)) => {
                        *last_cp = Some(CheckPoint::new(prev_id).push(this_id).expect("must push"))
                    }
                }

                emitter.last_block = Some(res);

                return Ok(Some((height, item)));
            }
            PollResponse::NoMoreBlocks => {
                emitter.last_block = None;
                return Ok(None);
            }
            PollResponse::BlockNotInBestChain => {
                emitter.last_block = None;
                continue;
            }
            PollResponse::AgreementFound(res, cp) => {
                let agreement_h = res.height as u32;

                // get rid of evicted blocks
                emitter.last_cp = Some(cp);

                // The tip during the last mempool emission needs to in the best chain, we reduce
                // it if it is not.
                if let Some(h) = emitter.last_mempool_tip.as_mut() {
                    if *h > agreement_h {
                        *h = agreement_h;
                    }
                }
                emitter.last_block = Some(res);
                continue;
            }
            PollResponse::AgreementPointNotFound => {
                // We want to clear `last_cp` and set `start_height` to the first checkpoint's
                // height. This way, the first checkpoint in `LocalChain` can be replaced.
                if let Some(last_cp) = emitter.last_cp.take() {
                    emitter.start_height = last_cp.height();
                }
                emitter.last_block = None;
                continue;
            }
        }
    }
}

/// Extends [`bitcoincore_rpc::Error`].
pub trait BitcoindRpcErrorExt {
    /// Returns whether the error is a "not found" error.
    ///
    /// This is useful since [`Emitter`] emits [`Result<_, bitcoincore_rpc::Error>`]s as
    /// [`Iterator::Item`].
    fn is_not_found_error(&self) -> bool;
}

impl BitcoindRpcErrorExt for bitcoincore_rpc::Error {
    fn is_not_found_error(&self) -> bool {
        if let bitcoincore_rpc::Error::JsonRpc(bitcoincore_rpc::jsonrpc::Error::Rpc(rpc_err)) = self
        {
            rpc_err.code == -5
        } else {
            false
        }
    }
}
