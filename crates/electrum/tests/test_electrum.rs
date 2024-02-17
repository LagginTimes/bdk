use anyhow::Result;
use bdk_chain::{
    bitcoin::{hashes::Hash, Address, Amount, ScriptBuf, Txid, WScriptHash},
    keychain::Balance,
    local_chain::{CheckPoint, LocalChain},
    BlockId, ConfirmationTimeHeightAnchor, IndexedTxGraph, SpkTxOutIndex,
};
use bdk_electrum::{ElectrumExt, ElectrumUpdate};
use bdk_testenv::TestEnv;
use electrsd::bitcoind::bitcoincore_rpc::RpcApi;

fn get_balance(
    recv_chain: &LocalChain,
    recv_graph: &IndexedTxGraph<ConfirmationTimeHeightAnchor, SpkTxOutIndex<()>>,
) -> Result<Balance> {
    let chain_tip = recv_chain.tip().block_id();
    let outpoints = recv_graph.index.outpoints().clone();
    let balance = recv_graph
        .graph()
        .balance(recv_chain, chain_tip, outpoints, |_, _| true);
    Ok(balance)
}

/// Ensure that [`ElectrumExt`] can sync properly.
///
/// 1. Mine 101 blocks.
/// 2. Send a tx.
/// 3. Mine extra block to confirm sent tx.
/// 4. Check [`Balance`] to ensure tx is confirmed.
#[test]
fn scan_detects_confirmed_tx() -> Result<()> {
    const SEND_AMOUNT: Amount = Amount::from_sat(10_000);

    let env = TestEnv::new()?;
    let client = electrum_client::Client::new(env.electrsd.electrum_url.as_str())?;

    // Setup addresses.
    let addr_to_mine = env
        .bitcoind
        .client
        .get_new_address(None, None)?
        .assume_checked();
    let spk_to_track = ScriptBuf::new_v0_p2wsh(&WScriptHash::all_zeros());
    let addr_to_track = Address::from_script(&spk_to_track, bdk_chain::bitcoin::Network::Regtest)?;

    // Setup receiver.
    let (mut recv_chain, _) = LocalChain::from_genesis_hash(env.bitcoind.client.get_block_hash(0)?);
    let mut recv_graph = IndexedTxGraph::<ConfirmationTimeHeightAnchor, _>::new({
        let mut recv_index = SpkTxOutIndex::default();
        recv_index.insert_spk((), spk_to_track.clone());
        recv_index
    });

    // Mine some blocks.
    env.mine_blocks(101, Some(addr_to_mine))?;

    // Create transaction that is tracked by our receiver.
    env.send(&addr_to_track, SEND_AMOUNT)?;

    // Mine a block to confirm sent tx.
    env.mine_blocks(1, None)?;

    // Sync up to tip.
    env.wait_until_electrum_sees_block()?;
    let ElectrumUpdate {
        chain_update,
        relevant_txids,
    } = client.sync(recv_chain.tip(), [spk_to_track], None, None, 5)?;

    let missing = relevant_txids.missing_full_txs(recv_graph.graph());
    let graph_update = relevant_txids.into_confirmation_time_tx_graph(&client, None, missing)?;
    let _ = recv_chain
        .apply_update(chain_update)
        .map_err(|err| anyhow::anyhow!("LocalChain update error: {:?}", err))?;
    let _ = recv_graph.apply_update(graph_update);

    // Check to see if tx is confirmed.
    assert_eq!(
        get_balance(&recv_chain, &recv_graph)?,
        Balance {
            confirmed: SEND_AMOUNT.to_sat(),
            ..Balance::default()
        },
    );

    Ok(())
}

#[test]
fn test_reorg_is_detected_in_electrsd() -> Result<()> {
    let env = TestEnv::new()?;

    // Mine some blocks.
    env.mine_blocks(101, None)?;
    env.wait_until_electrum_sees_block()?;
    let height = env.bitcoind.client.get_block_count()?;
    let blocks = (0..=height)
        .map(|i| env.bitcoind.client.get_block_hash(i))
        .collect::<Result<Vec<_>, _>>()?;

    // Perform reorg on six blocks.
    env.reorg(6)?;
    env.wait_until_electrum_sees_block()?;
    let reorged_height = env.bitcoind.client.get_block_count()?;
    let reorged_blocks = (0..=height)
        .map(|i| env.bitcoind.client.get_block_hash(i))
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(height, reorged_height);

    // Block hashes should not be equal on the six reorged blocks.
    for (i, (block, reorged_block)) in blocks.iter().zip(reorged_blocks.iter()).enumerate() {
        match i <= height as usize - 6 {
            true => assert_eq!(block, reorged_block),
            false => assert_ne!(block, reorged_block),
        }
    }

    Ok(())
}

/// Ensure that confirmed txs that are reorged become unconfirmed.
///
/// 1. Mine 101 blocks.
/// 2. Mine 11 blocks with a confirmed tx in each.
/// 3. Perform 11 separate reorgs on each block with a confirmed tx.
/// 4. Check [`Balance`] after each reorg to ensure unconfirmed amount is correct.
#[test]
fn tx_can_become_unconfirmed_after_reorg() -> Result<()> {
    const REORG_COUNT: usize = 8;
    const SEND_AMOUNT: Amount = Amount::from_sat(10_000);

    let env = TestEnv::new()?;
    let client = electrum_client::Client::new(env.electrsd.electrum_url.as_str())?;

    // Setup addresses.
    let addr_to_mine = env
        .bitcoind
        .client
        .get_new_address(None, None)?
        .assume_checked();
    let spk_to_track = ScriptBuf::new_v0_p2wsh(&WScriptHash::all_zeros());
    let addr_to_track = Address::from_script(&spk_to_track, bdk_chain::bitcoin::Network::Regtest)?;

    // Setup receiver.
    let (mut recv_chain, _) = LocalChain::from_genesis_hash(env.bitcoind.client.get_block_hash(0)?);
    let mut recv_graph = IndexedTxGraph::<ConfirmationTimeHeightAnchor, _>::new({
        let mut recv_index = SpkTxOutIndex::default();
        recv_index.insert_spk((), spk_to_track.clone());
        recv_index
    });

    // Mine some blocks.
    env.mine_blocks(101, Some(addr_to_mine))?;

    // Create transactions that are tracked by our receiver.
    for _ in 0..REORG_COUNT {
        env.send(&addr_to_track, SEND_AMOUNT)?;
        env.mine_blocks(1, None)?;
    }

    // Sync up to tip.
    env.wait_until_electrum_sees_block()?;
    let ElectrumUpdate {
        chain_update,
        relevant_txids,
    } = client.sync(recv_chain.tip(), [spk_to_track.clone()], None, None, 5)?;

    let missing = relevant_txids.missing_full_txs(recv_graph.graph());
    let graph_update = relevant_txids.into_confirmation_time_tx_graph(&client, None, missing)?;
    let _ = recv_chain
        .apply_update(chain_update)
        .map_err(|err| anyhow::anyhow!("LocalChain update error: {:?}", err))?;
    let _ = recv_graph.apply_update(graph_update.clone());

    // Retain a snapshot of all anchors before reorg process.
    let initial_anchors = graph_update.all_anchors();

    // Check if initial balance is correct.
    assert_eq!(
        get_balance(&recv_chain, &recv_graph)?,
        Balance {
            confirmed: SEND_AMOUNT.to_sat() * REORG_COUNT as u64,
            ..Balance::default()
        },
        "initial balance must be correct",
    );

    // Perform reorgs with different depths.
    for depth in 1..=REORG_COUNT {
        env.reorg_empty_blocks(depth)?;

        env.wait_until_electrum_sees_block()?;
        let ElectrumUpdate {
            chain_update,
            relevant_txids,
        } = client.sync(recv_chain.tip(), [spk_to_track.clone()], None, None, 5)?;

        let missing = relevant_txids.missing_full_txs(recv_graph.graph());
        let graph_update =
            relevant_txids.into_confirmation_time_tx_graph(&client, None, missing)?;
        let _ = recv_chain
            .apply_update(chain_update)
            .map_err(|err| anyhow::anyhow!("LocalChain update error: {:?}", err))?;

        // Check to see if a new anchor is added during current reorg.
        if !initial_anchors.is_superset(graph_update.all_anchors()) {
            println!("New anchor added at reorg depth {}", depth);
        }
        let _ = recv_graph.apply_update(graph_update);

        assert_eq!(
            get_balance(&recv_chain, &recv_graph)?,
            Balance {
                confirmed: SEND_AMOUNT.to_sat() * (REORG_COUNT - depth) as u64,
                trusted_pending: SEND_AMOUNT.to_sat() * depth as u64,
                ..Balance::default()
            },
            "reorg_count: {}",
            depth,
        );
    }

    Ok(())
}

#[test]
fn update_tx_graph_gap_limit() -> Result<()> {
    use std::collections::{BTreeMap, HashSet};
    use std::str::FromStr;

    let env = TestEnv::new()?;
    let client = electrum_client::Client::new(env.electrsd.electrum_url.as_str())?;

    // Now let's test the gap limit. First get 10 new addresses and index them.
    let addresses: Vec<Address> = [
        "bcrt1qj9f7r8r3p2y0sqf4r3r62qysmkuh0fzep473d2ar7rcz64wqvhssjgf0z4",
        "bcrt1qmm5t0ch7vh2hryx9ctq3mswexcugqe4atkpkl2tetm8merqkthas3w7q30",
        "bcrt1qut9p7ej7l7lhyvekj28xknn8gnugtym4d5qvnp5shrsr4nksmfqsmyn87g",
        "bcrt1qqz0xtn3m235p2k96f5wa2dqukg6shxn9n3txe8arlrhjh5p744hsd957ww",
        "bcrt1q9c0t62a8l6wfytmf2t9lfj35avadk3mm8g4p3l84tp6rl66m48sqrme7wu",
        "bcrt1qkmh8yrk2v47cklt8dytk8f3ammcwa4q7dzattedzfhqzvfwwgyzsg59zrh",
        "bcrt1qvgrsrzy07gjkkfr5luplt0azxtfwmwq5t62gum5jr7zwcvep2acs8hhnp2",
        "bcrt1qw57edarcg50ansq8mk3guyrk78rk0fwvrds5xvqeupteu848zayq549av8",
        "bcrt1qvtve5ekf6e5kzs68knvnt2phfw6a0yjqrlgat392m6zt9jsvyxhqfx67ef",
        "bcrt1qw03ddumfs9z0kcu76ln7jrjfdwam20qtffmkcral3qtza90sp9kqm787uk",
    ]
    .into_iter()
    .map(|s| Address::from_str(s).unwrap().assume_checked())
    .collect();
    let spks: Vec<(u32, ScriptBuf)> = addresses
        .iter()
        .enumerate()
        .map(|(i, addr)| (i as u32, addr.script_pubkey()))
        .collect();

    let mut keychain_spks = BTreeMap::new();
    keychain_spks.insert(0, spks);
    let tx_graph = IndexedTxGraph::<ConfirmationTimeHeightAnchor, _>::new({
        let mut index = SpkTxOutIndex::default();
        for (i, spk) in keychain_spks.get(&0).unwrap() {
            index.insert_spk(i, spk.clone());
        }
        index
    });

    // Mine blocks.
    let block_hashes = env.mine_blocks(101, None)?;
    let prev_tip = CheckPoint::new(BlockId {
        height: 1,
        hash: block_hashes[0],
    });

    // Then receive coins on the 4th address.
    let txid_4th_addr = env.bitcoind.client.send_to_address(
        &addresses[3],
        Amount::from_sat(10000),
        None,
        None,
        None,
        None,
        Some(1),
        None,
    )?;
    let _ = env.mine_blocks(1, None)?;
    env.wait_until_electrum_sees_block()?;

    // A scan with a gap limit of 2 won't find the transaction, but a scan with a gap limit of 3 will.
    // FIXME: See <http://github.com/bitcoindevkit/bdk/pull/1351> which changes the behavior of `stop_gap`
    let (ElectrumUpdate { relevant_txids, .. }, active_indices) =
        client.full_scan(prev_tip.clone(), keychain_spks.clone(), 2, 1)?;
    let missing = relevant_txids.missing_full_txs(tx_graph.graph());
    let graph_update = relevant_txids.into_confirmation_time_tx_graph(&client, None, missing)?;
    assert!(graph_update.full_txs().next().is_none());
    assert!(active_indices.is_empty());

    let (ElectrumUpdate { relevant_txids, .. }, active_indices) =
        client.full_scan(prev_tip.clone(), keychain_spks.clone(), 3, 1)?;
    let missing = relevant_txids.missing_full_txs(tx_graph.graph());
    let graph_update = relevant_txids.into_confirmation_time_tx_graph(&client, None, missing)?;
    assert_eq!(graph_update.full_txs().next().unwrap().txid, txid_4th_addr);
    assert_eq!(active_indices[&0], 3);

    // Now receive a coin on the last address.
    let txid_last_addr = env.bitcoind.client.send_to_address(
        &addresses[addresses.len() - 1],
        Amount::from_sat(10000),
        None,
        None,
        None,
        None,
        Some(1),
        None,
    )?;
    let _ = env.mine_blocks(1, None)?;
    env.wait_until_electrum_sees_block()?;

    // A scan with gap limit 4 won't find the second transaction, but a scan with gap limit 5 will.
    // The last active index won't be updated in the first case but will in the second.
    let (ElectrumUpdate { relevant_txids, .. }, active_indices) =
        client.full_scan(prev_tip.clone(), keychain_spks.clone(), 4, 1)?;
    let missing = relevant_txids.missing_full_txs(tx_graph.graph());
    let graph_update = relevant_txids.into_confirmation_time_tx_graph(&client, None, missing)?;
    let txids: HashSet<Txid> = graph_update.full_txs().map(|tx| tx.txid).collect();
    assert_eq!(txids.len(), 1);
    assert!(txids.contains(&txid_4th_addr));
    assert_eq!(active_indices[&0], 3);

    let (ElectrumUpdate { relevant_txids, .. }, active_indices) =
        client.full_scan(prev_tip, keychain_spks.clone(), 5, 1)?;
    let missing = relevant_txids.missing_full_txs(tx_graph.graph());
    let graph_update = relevant_txids.into_confirmation_time_tx_graph(&client, None, missing)?;
    let txids: HashSet<Txid> = graph_update.full_txs().map(|tx| tx.txid).collect();
    assert_eq!(txids.len(), 2);
    assert!(txids.contains(&txid_4th_addr) && txids.contains(&txid_last_addr));
    assert_eq!(active_indices[&0], 9);

    Ok(())
}
