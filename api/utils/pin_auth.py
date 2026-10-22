"""Pure PIN client-daemon signature helpers.

Wire contract (shared with pin-clientd and Tauri PIN client):
    secret_hash = sha256(api_secret)
    signature   = sha256(client_id + timestamp + secret_hash)

The server stores api_key_hash = sha256(raw api secret). That hash is the
authoritative credential used by Bearer auth, so WebSocket verification must
use the same field rather than independently decrypting a potentially stale
api_key_encrypted copy.
"""
from __future__ import annotations

import hashlib
import hmac


def hash_api_secret(api_secret: str) -> str:
    return hashlib.sha256(api_secret.encode()).hexdigest()


def signature_from_hash(client_id: str, timestamp: str,
                        secret_hash: str) -> str:
    payload = f"{client_id}{timestamp}{secret_hash}".encode()
    return hashlib.sha256(payload).hexdigest()


def signature_from_secret(client_id: str, timestamp: str,
                          api_secret: str) -> str:
    return signature_from_hash(client_id, timestamp, hash_api_secret(api_secret))


def verify_signature(client_id: str, timestamp: str, signature: str,
                     secret_hash: str) -> bool:
    expected = signature_from_hash(client_id, timestamp, secret_hash)
    return hmac.compare_digest(signature, expected)
