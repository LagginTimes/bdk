use bdk_chain::bitcoin::{Address, Amount, BlockHash, Txid};
use bitcoin::{
    address::NetworkChecked, block::Header, hash_types::TxMerkleNode, hashes::Hash,
    secp256k1::rand::random, Block, CompactTarget, ScriptBuf, ScriptHash, Transaction, TxIn, TxOut,
};
use bitcoincore_rpc::{
    bitcoincore_rpc_json::{GetBlockTemplateModes, GetBlockTemplateRules},
    RpcApi,
};

pub struct TestEnv {
    #[allow(dead_code)]
    pub daemon: bitcoind::BitcoinD,
    pub client: bitcoincore_rpc::Client,
}

impl TestEnv {
    pub fn new() -> anyhow::Result<Self> {
        let daemon = match std::env::var_os("TEST_BITCOIND") {
            Some(bitcoind_path) => bitcoind::BitcoinD::new(bitcoind_path),
            None => bitcoind::BitcoinD::from_downloaded(),
        }?;
        let client = bitcoincore_rpc::Client::new(
            &daemon.rpc_url(),
            bitcoincore_rpc::Auth::CookieFile(daemon.params.cookie_file.clone()),
        )?;
        Ok(Self { daemon, client })
    }

    pub fn mine_blocks(
        &self,
        count: usize,
        address: Option<Address>,
    ) -> anyhow::Result<Vec<BlockHash>> {
        let coinbase_address = match address {
            Some(address) => address,
            None => self.client.get_new_address(None, None)?.assume_checked(),
        };
        let block_hashes = self
            .client
            .generate_to_address(count as _, &coinbase_address)?;
        Ok(block_hashes)
    }

    pub fn mine_empty_block(&self) -> anyhow::Result<(usize, BlockHash)> {
        let bt = self.client.get_block_template(
            GetBlockTemplateModes::Template,
            &[GetBlockTemplateRules::SegWit],
            &[],
        )?;

        let txdata = vec![Transaction {
            version: 1,
            lock_time: bitcoin::absolute::LockTime::from_height(0)?,
            input: vec![TxIn {
                previous_output: bitcoin::OutPoint::default(),
                script_sig: ScriptBuf::builder()
                    .push_int(bt.height as _)
                    // randomn number so that re-mining creates unique block
                    .push_int(random())
                    .into_script(),
                sequence: bitcoin::Sequence::default(),
                witness: bitcoin::Witness::new(),
            }],
            output: vec![TxOut {
                value: 0,
                script_pubkey: ScriptBuf::new_p2sh(&ScriptHash::all_zeros()),
            }],
        }];

        let bits: [u8; 4] = bt
            .bits
            .clone()
            .try_into()
            .expect("rpc provided us with invalid bits");

        let mut block = Block {
            header: Header {
                version: bitcoin::block::Version::default(),
                prev_blockhash: bt.previous_block_hash,
                merkle_root: TxMerkleNode::all_zeros(),
                time: Ord::max(bt.min_time, std::time::UNIX_EPOCH.elapsed()?.as_secs()) as u32,
                bits: CompactTarget::from_consensus(u32::from_be_bytes(bits)),
                nonce: 0,
            },
            txdata,
        };

        block.header.merkle_root = block.compute_merkle_root().expect("must compute");

        for nonce in 0..=u32::MAX {
            block.header.nonce = nonce;
            if block.header.target().is_met_by(block.block_hash()) {
                break;
            }
        }

        self.client.submit_block(&block)?;
        Ok((bt.height as usize, block.block_hash()))
    }

    pub fn invalidate_blocks(&self, count: usize) -> anyhow::Result<()> {
        let mut hash = self.client.get_best_block_hash()?;
        for _ in 0..count {
            let prev_hash = self.client.get_block_info(&hash)?.previousblockhash;
            self.client.invalidate_block(&hash)?;
            match prev_hash {
                Some(prev_hash) => hash = prev_hash,
                None => break,
            }
        }
        Ok(())
    }

    pub fn reorg(&self, count: usize) -> anyhow::Result<Vec<BlockHash>> {
        let start_height = self.client.get_block_count()?;
        self.invalidate_blocks(count)?;

        let res = self.mine_blocks(count, None);
        assert_eq!(
            self.client.get_block_count()?,
            start_height,
            "reorg should not result in height change"
        );
        res
    }

    pub fn reorg_empty_blocks(&self, count: usize) -> anyhow::Result<Vec<(usize, BlockHash)>> {
        let start_height = self.client.get_block_count()?;
        self.invalidate_blocks(count)?;

        let res = (0..count)
            .map(|_| self.mine_empty_block())
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            self.client.get_block_count()?,
            start_height,
            "reorg should not result in height change"
        );
        Ok(res)
    }

    pub fn send(&self, address: &Address<NetworkChecked>, amount: Amount) -> anyhow::Result<Txid> {
        let txid = self
            .client
            .send_to_address(address, amount, None, None, None, None, None, None)?;
        Ok(txid)
    }
}
