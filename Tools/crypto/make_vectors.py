# /// script
# requires-python = ">=3.10"
# dependencies = ["pynacl==1.6.2"]
# ///
"""Writes Tests/ZMTPCryptoTests/Fixtures/vectors.json: known answers from libsodium.

PyNaCl wraps libsodium, so these are the values a real CurveZMQ peer computes.
The inputs are fixed (derived from labels with SHA-256), so a re-run gives the same file.

    uv run Tools/crypto/make_vectors.py
"""

import hashlib
import json
import pathlib

import nacl
import nacl.bindings
import nacl.public
import nacl.secret


def det(label: str, length: int) -> bytes:
    """`length` deterministic bytes for `label` (SHA-256 in counter mode)."""
    out = b""
    counter = 0
    while len(out) < length:
        out += hashlib.sha256(f"swift-zmtp/{label}/{counter}".encode()).digest()
        counter += 1
    return out[:length]


def hx(data: bytes) -> str:
    return data.hex()


def keypair(label: str) -> tuple[bytes, bytes]:
    """(secret, public) for a fixed secret; the public key comes from libsodium."""
    secret = det(f"{label}/secret", 32)
    return secret, nacl.bindings.crypto_scalarmult_base(secret)


vectors: dict = {
    "generator": "Tools/crypto/make_vectors.py",
    "pynacl": nacl.__version__,  # bundles libsodium
}

# crypto_scalarmult_base: secret -> public.
vectors["scalarmult_base"] = []
for name in ("a", "b", "c"):
    secret, public = keypair(f"scalarmult_base/{name}")
    vectors["scalarmult_base"].append({"secret": hx(secret), "public": hx(public)})

# crypto_scalarmult: shared secret of two key pairs; both directions give the same value.
vectors["scalarmult"] = []
for name in ("ab", "cd"):
    sk_a, pk_a = keypair(f"scalarmult/{name}/a")
    sk_b, pk_b = keypair(f"scalarmult/{name}/b")
    shared = nacl.bindings.crypto_scalarmult(sk_a, pk_b)
    assert shared == nacl.bindings.crypto_scalarmult(sk_b, pk_a)
    vectors["scalarmult"].append(
        {"secret": hx(sk_a), "public": hx(pk_b), "shared": hx(shared)}
    )
    vectors["scalarmult"].append(
        {"secret": hx(sk_b), "public": hx(pk_a), "shared": hx(shared)}
    )

# crypto_box_beforenm: HSalsa20(X25519(secret, public), 16 zero bytes).
vectors["box_beforenm"] = []
for name in ("x", "y"):
    sk_a, pk_a = keypair(f"beforenm/{name}/a")
    sk_b, pk_b = keypair(f"beforenm/{name}/b")
    key = nacl.bindings.crypto_box_beforenm(pk_b, sk_a)
    assert key == nacl.bindings.crypto_box_beforenm(pk_a, sk_b)
    vectors["box_beforenm"].append(
        {"secret": hx(sk_a), "public": hx(pk_b), "key": hx(key)}
    )

# crypto_secretbox_easy: 16-byte MAC, then the ciphertext.
vectors["secretbox_easy"] = []
for length in (0, 1, 64, 1000):
    key = det(f"secretbox/{length}/key", 32)
    nonce = det(f"secretbox/{length}/nonce", 24)
    plaintext = det(f"secretbox/{length}/plaintext", length)
    box = nacl.secret.SecretBox(key).encrypt(plaintext, nonce).ciphertext
    assert len(box) == length + 16
    vectors["secretbox_easy"].append(
        {"key": hx(key), "nonce": hx(nonce), "plaintext": hx(plaintext), "box": hx(box)}
    )

# crypto_box_easy: sender secret + recipient public -> box. Checks beforenm + secretbox together.
vectors["box_easy"] = []
for length in (0, 33, 200):
    sender_sk, sender_pk = keypair(f"box/{length}/sender")
    recipient_sk, recipient_pk = keypair(f"box/{length}/recipient")
    nonce = det(f"box/{length}/nonce", 24)
    plaintext = det(f"box/{length}/plaintext", length)
    box = nacl.public.Box(
        nacl.public.PrivateKey(sender_sk), nacl.public.PublicKey(recipient_pk)
    ).encrypt(plaintext, nonce).ciphertext
    opened = nacl.public.Box(
        nacl.public.PrivateKey(recipient_sk), nacl.public.PublicKey(sender_pk)
    ).decrypt(box, nonce)
    assert opened == plaintext
    vectors["box_easy"].append(
        {
            "sender_secret": hx(sender_sk),
            "sender_public": hx(sender_pk),
            "recipient_secret": hx(recipient_sk),
            "recipient_public": hx(recipient_pk),
            "nonce": hx(nonce),
            "plaintext": hx(plaintext),
            "box": hx(box),
        }
    )

out = (
    pathlib.Path(__file__).resolve().parents[2]
    / "Tests/ZMTPCryptoTests/Fixtures/vectors.json"
)
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps(vectors, indent=2) + "\n")
print(f"wrote {out}")
