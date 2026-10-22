"""PIN daemon/server signature contract regression tests."""
from api.utils.pin_auth import (
    hash_api_secret,
    signature_from_hash,
    signature_from_secret,
    verify_signature,
)


CLIENT_ID = "op_test"
TIMESTAMP = "1704499200"
API_SECRET = "pin_sk_example"
SECRET_HASH = "322f88c530aea6726ae4c0f4327e84ee7140b09aba4c4e55b0f5ba137db0aed4"
SIGNATURE = "dae80712edd88b902076fcd5746c8788b04ee6f6fe6bcb644be0be4325d92898"


def test_known_vector_matches_pin_clientd_wire_contract():
    assert hash_api_secret(API_SECRET) == SECRET_HASH
    assert signature_from_secret(CLIENT_ID, TIMESTAMP, API_SECRET) == SIGNATURE
    assert signature_from_hash(CLIENT_ID, TIMESTAMP, SECRET_HASH) == SIGNATURE
    assert verify_signature(CLIENT_ID, TIMESTAMP, SIGNATURE, SECRET_HASH)


def test_wrong_secret_and_modified_payload_are_rejected():
    wrong_hash = hash_api_secret("pin_sk_stale_or_rotated")
    assert not verify_signature(CLIENT_ID, TIMESTAMP, SIGNATURE, wrong_hash)
    assert not verify_signature("op_other", TIMESTAMP, SIGNATURE, SECRET_HASH)
    assert not verify_signature(CLIENT_ID, "1704499201", SIGNATURE, SECRET_HASH)
    assert not verify_signature(CLIENT_ID, TIMESTAMP, "0" * 64, SECRET_HASH)
