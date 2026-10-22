import os
import base64
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

_fernet_instance = None

def _get_encryption_key() -> bytes:
    secret = os.getenv("ENCRYPTION_KEY", os.getenv("SESSION_SECRET", "dev-encryption-key"))
    salt = b"aiassist_provider_keys_v1"
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=100000,
    )
    key = base64.urlsafe_b64encode(kdf.derive(secret.encode()))
    return key

def get_fernet() -> Fernet:
    global _fernet_instance
    if _fernet_instance is None:
        _fernet_instance = Fernet(_get_encryption_key())
    return _fernet_instance

def encrypt_api_key(plaintext_key: str) -> str:
    fernet = get_fernet()
    encrypted = fernet.encrypt(plaintext_key.encode())
    return base64.urlsafe_b64encode(encrypted).decode()

def decrypt_api_key(encrypted_key: str) -> str:
    fernet = get_fernet()
    encrypted_bytes = base64.urlsafe_b64decode(encrypted_key.encode())
    decrypted = fernet.decrypt(encrypted_bytes)
    return decrypted.decode()

def validate_groq_key_format(api_key: str) -> bool:
    if not api_key:
        return False
    if len(api_key) < 20:
        return False
    return True
