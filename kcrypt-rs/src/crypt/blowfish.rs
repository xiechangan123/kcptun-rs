//! Blowfish — 8-byte block cipher.
//!
//! Uses RustCrypto `blowfish` crate. Operates in CFB-8 mode.
//! The cipher instance is created ONCE in the constructor and reused for
//! every block — re-creating it per block (key schedule) was the original
//! performance bug that made Blowfish ~100x slower than Go.
//!
//! Hot path: monomorphized CFB (no `Fn` closure) + reuse of a single
//! `GenericArray` buffer for block encrypt.

use super::{cfb8_decrypt, cfb8_encrypt, BlockCipher8, BlockCrypt};
use blowfish::cipher::{generic_array::GenericArray, BlockEncrypt, KeyInit};
use blowfish::Blowfish;

#[derive(Debug)]
pub struct BlowfishCrypt {
    cipher: Blowfish,
}

impl BlowfishCrypt {
    /// Create a Blowfish engine. Fail-fast on invalid key length (P2 / L-6):
    /// 57–72 byte keys used to `panic!`. The `blowfish` crate accepts 4–56;
    /// Go's accepts up to 72 — anything outside the crate's range is rejected
    /// cleanly rather than aborting the process.
    pub fn new(key: &[u8]) -> Result<Self, &'static str> {
        if !(4..=56).contains(&key.len()) {
            return Err("blowfish: key must be 4–56 bytes");
        }
        let cipher =
            <Blowfish as KeyInit>::new_from_slice(key).map_err(|_| "blowfish: invalid key")?;
        Ok(BlowfishCrypt { cipher })
    }

    /// Encrypt one 8-byte block in place (CFB register / ciphertext feedback).
    #[inline(always)]
    fn encrypt_block_inplace(&self, block: &mut [u8; 8]) {
        let ga = GenericArray::from_mut_slice(block);
        self.cipher.encrypt_block(ga);
    }

    /// Encrypt one 8-byte block (BlockCipher8): out = E(inp).
    #[inline(always)]
    fn encrypt_block(&self, out: &mut [u8; 8], inp: &[u8; 8]) {
        out.copy_from_slice(inp);
        self.encrypt_block_inplace(out);
    }
}

impl BlockCipher8 for BlowfishCrypt {
    #[inline]
    fn encrypt_block(&self, out: &mut [u8; 8], inp: &[u8; 8]) {
        self.encrypt_block(out, inp);
    }
}

impl BlockCrypt for BlowfishCrypt {
    fn encrypt(&self, data: &mut [u8]) {
        cfb8_encrypt(data, self);
    }
    fn decrypt(&self, data: &mut [u8]) {
        cfb8_decrypt(data, self);
    }
    fn name(&self) -> &'static str {
        "blowfish"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn rt(c: &dyn BlockCrypt, d: &mut [u8]) {
        let o = d.to_vec();
        c.encrypt(d);
        c.decrypt(d);
        assert_eq!(d, &o, "{} roundtrip", c.name());
    }
    #[test]
    fn bfish() {
        rt(
            &BlowfishCrypt::new(b"test-key").unwrap(),
            &mut b"hello kcp bf!".to_vec(),
        );
    }
}
