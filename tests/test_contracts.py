import json
from copy import deepcopy

import pytest

from common.contracts import (
    REQUIRED_COLUMNS,
    ValidationError,
    parse_json,
    validate_score,
    validate_transaction,
)


@pytest.fixture
def transaction():
    return {
        "transaction_id": "sample-001",
        "data": {
            "transaction_time": "2019-09-14 02:46",
            "merch": "fraud_Stokes, Christiansen and Sipes",
            "cat_id": "grocery_net",
            "amount": "25.79",
            "name_1": "Michael",
            "name_2": "Rodriguez",
            "gender": "M",
            "street": "172 Paula Inlet Apt. 650",
            "one_city": "Cross Plains",
            "us_state": "TX",
            "post_code": 76443,
            "lat": "32.1482",
            "lon": "-99.1872",
            "population_city": 1897,
            "jobs": "Chief Operating Officer",
            "merchant_lat": "31.772057",
            "merchant_lon": "-99.103183",
        },
    }


def test_csv_row_is_normalized_without_mutating_input(transaction):
    original = deepcopy(transaction)
    result = validate_transaction(transaction)
    assert set(result["data"]) == set(REQUIRED_COLUMNS)
    assert result["data"]["amount"] == 25.79
    assert result["data"]["post_code"] == "76443"
    assert transaction == original


@pytest.mark.parametrize("field,bad_value", [
    ("amount", -1), ("amount", "NaN"), ("amount", True),
    ("amount", None), ("population_city", -1), ("lat", 90.1),
    ("lon", -180.1), ("merchant_lat", float("inf")),
    ("merchant_lon", 180.01), ("transaction_time", "2019-02-30 12:00"),
    ("name_1", None), ("street", ""), ("post_code", False),
    ("post_code", "not-a-number"), ("post_code", "NaN"),
    ("post_code", "999999"), ("post_code", "１２３４５"),
])
def test_invalid_row_fields_are_rejected(transaction, field, bad_value):
    transaction["data"][field] = bad_value
    with pytest.raises(ValidationError, match=field):
        validate_transaction(transaction)


def test_missing_column_names_are_reported(transaction):
    del transaction["data"]["cat_id"]
    with pytest.raises(ValidationError, match="Missing required fields: cat_id"):
        validate_transaction(transaction)


@pytest.mark.parametrize("transaction_id", [None, 1, "", "   ", "a" * 129])
def test_invalid_ids_are_rejected(transaction, transaction_id):
    transaction["transaction_id"] = transaction_id
    with pytest.raises(ValidationError, match="transaction_id"):
        validate_transaction(transaction)


@pytest.mark.parametrize("payload", [None, b"\xff", b"{", "[]", "null", "42"])
def test_non_object_or_malformed_json_is_rejected(payload):
    with pytest.raises(ValidationError):
        parse_json(payload)


def test_json_round_trip_works(transaction):
    assert parse_json(json.dumps(transaction).encode()) == transaction


def test_errors_do_not_include_private_values(transaction):
    secret = "private-customer-address"
    transaction["data"]["amount"] = secret
    with pytest.raises(ValidationError) as failure:
        validate_transaction(transaction)
    assert secret not in str(failure.value)


@pytest.mark.parametrize("score", [0, 0.3, 1])
def test_score_contract_has_exactly_four_fields_and_float_score(score):
    result = validate_score({"transaction_id": "t1", "score": score, "fraud_flag": 1, "model_version": "v1"})
    assert set(result) == {"transaction_id", "score", "fraud_flag", "model_version"}
    assert type(result["score"]) is float


@pytest.mark.parametrize("updates", [
    {"score": -0.01}, {"score": 1.01}, {"score": float("nan")},
    {"score": float("inf")}, {"score": "0.5"}, {"score": True},
    {"fraud_flag": 2}, {"fraud_flag": True}, {"fraud_flag": 1.0},
    {"fraud_flag": "1"}, {"extra": "not allowed"}, {"model_version": ""}, {"model_version": None},
])
def test_bad_score_events_are_rejected(updates):
    score = {"transaction_id": "t1", "score": 0.5, "fraud_flag": 1, "model_version": "v1"}
    score.update(updates)
    with pytest.raises(ValidationError):
        validate_score(score)


def test_legacy_array_of_scores_is_not_a_score():
    with pytest.raises(ValidationError):
        validate_score([{"transaction_id": "t1", "score": 0.5, "fraud_flag": 1, "model_version": "v1"}])
