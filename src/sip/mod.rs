// This file is Copyright its original authors, visible in version control history.
//
// This file is licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. You may not use this file except in
// accordance with one or both of these licenses.

//! Swap-in-potentiam wallet and state management.
//!
//! This module provides a BDK-integrated wallet for managing swap-in-potentiam (SIP) addresses
//! and UTXOs. It leverages the address construction primitives from `lightning_liquidity::sip` and
//! adds wallet-level functionality: key derivation, UTXO discovery via chain sync, state tracking,
//! and transaction building for refund and cooperative spends.

pub(crate) mod state;
pub(crate) mod wallet;
