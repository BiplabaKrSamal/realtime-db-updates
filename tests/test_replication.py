import struct
import sys
import pathlib

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "server"))

from app.db.replication import (
    _decode_message,
    _decode_tuple,
    _decode_text_datum,
    _build_payload,
    _read_cstring,
    _relations,
)


def _pack_cstring(s: str) -> bytes:
    return s.encode() + b"\x00"


def _pack_text_datum(value: str | None) -> bytes:
    if value is None:
        return b"n"
    encoded = value.encode("utf-8")
    return b"t" + struct.pack(">I", len(encoded)) + encoded


def _pack_tuple(values: list) -> bytes:
    out = struct.pack(">H", len(values))
    for v in values:
        out += _pack_text_datum(v)
    return out


def _make_relation_msg(rel_id: int, table: str, columns: list[str]) -> bytes:
    msg  = b"R"
    msg += struct.pack(">I", rel_id)
    msg += _pack_cstring("public")
    msg += _pack_cstring(table)
    msg += b"d"
    msg += struct.pack(">H", len(columns))
    for col in columns:
        msg += b"\x00"
        msg += _pack_cstring(col)
        msg += struct.pack(">I", 23)
        msg += struct.pack(">I", -1)
    return msg


def _make_insert_msg(rel_id: int, values: list) -> bytes:
    msg  = b"I"
    msg += struct.pack(">I", rel_id)
    msg += b"N"
    msg += _pack_tuple(values)
    return msg


def _make_update_msg(rel_id: int, new_values: list, old_values: list | None = None) -> bytes:
    msg = b"U"
    msg += struct.pack(">I", rel_id)
    if old_values is not None:
        msg += b"O"
        msg += _pack_tuple(old_values)
    msg += b"N"
    msg += _pack_tuple(new_values)
    return msg


def _make_delete_msg(rel_id: int, old_values: list) -> bytes:
    msg  = b"D"
    msg += struct.pack(">I", rel_id)
    msg += b"O"
    msg += _pack_tuple(old_values)
    return msg


ORDERS_REL_ID = 42
ORDERS_COLS = ["id", "customer_name", "product_name", "status", "updated_at"]


@pytest.fixture(autouse=True)
def seed_relation():
    _relations.clear()
    _relations[ORDERS_REL_ID] = {"columns": ORDERS_COLS}
    yield
    _relations.clear()


class TestReadCstring:
    def test_simple(self):
        val, pos = _read_cstring(b"hello\x00world", 0)
        assert val == "hello"
        assert pos == 6

    def test_empty_string(self):
        val, pos = _read_cstring(b"\x00rest", 0)
        assert val == ""
        assert pos == 1

    def test_offset(self):
        val, pos = _read_cstring(b"skip\x00name\x00", 5)
        assert val == "name"
        assert pos == 10


class TestDecodeTextDatum:
    def test_null(self):
        val, pos = _decode_text_datum(b"n", 0)
        assert val is None
        assert pos == 1

    def test_unchanged_toast(self):
        val, pos = _decode_text_datum(b"u", 0)
        assert val == "__unchanged__"
        assert pos == 1

    def test_text_value(self):
        payload = _pack_text_datum("hello")
        val, pos = _decode_text_datum(payload, 0)
        assert val == "hello"
        assert pos == 1 + 4 + len("hello")

    def test_unicode_value(self):
        text = "こんにちは"
        val, _ = _decode_text_datum(_pack_text_datum(text), 0)
        assert val == text


class TestDecodeRelation:
    def test_populates_relation_cache(self):
        _relations.clear()
        result = _decode_message(_make_relation_msg(99, "orders", ["id", "status"]))
        assert result is None
        assert _relations[99]["columns"] == ["id", "status"]

    def test_multiple_tables(self):
        _relations.clear()
        _decode_message(_make_relation_msg(1, "orders",   ["id", "status"]))
        _decode_message(_make_relation_msg(2, "products", ["id", "name"]))
        assert _relations[1]["columns"] == ["id", "status"]
        assert _relations[2]["columns"] == ["id", "name"]


class TestDecodeInsert:
    def test_basic_insert(self):
        result = _decode_message(_make_insert_msg(ORDERS_REL_ID, ["1", "Alice", "Headphones", "pending", "2024-01-01"]))
        assert result["tag"] == "insert"
        assert result["old"] is None
        assert result["new"]["id"] == "1"
        assert result["new"]["customer_name"] == "Alice"
        assert result["new"]["status"] == "pending"

    def test_insert_with_null_field(self):
        result = _decode_message(_make_insert_msg(ORDERS_REL_ID, ["2", "Bob", None, "shipped", "2024-01-01"]))
        assert result["new"]["product_name"] is None

    def test_insert_unknown_relation_returns_empty_row(self):
        result = _decode_message(_make_insert_msg(999, ["5", "X", "Y", "pending", "now"]))
        assert result["tag"] == "insert"
        assert result["new"] == {}


class TestDecodeUpdate:
    def test_update_with_old_row(self):
        old = ["3", "Carol", "Keyboard", "pending", "2024-01-01"]
        new = ["3", "Carol", "Keyboard", "shipped", "2024-01-02"]
        result = _decode_message(_make_update_msg(ORDERS_REL_ID, new, old))
        assert result["tag"] == "update"
        assert result["old"]["status"] == "pending"
        assert result["new"]["status"] == "shipped"

    def test_update_without_old_row(self):
        result = _decode_message(_make_update_msg(ORDERS_REL_ID, ["4", "Dave", "Monitor", "delivered", "2024-01-03"]))
        assert result["tag"] == "update"
        assert result["old"] is None
        assert result["new"]["status"] == "delivered"

    def test_update_preserves_all_columns(self):
        old = ["7", "Eva", "Webcam", "pending", "t1"]
        new = ["7", "Eva", "Webcam", "shipped", "t2"]
        result = _decode_message(_make_update_msg(ORDERS_REL_ID, new, old))
        assert list(result["new"].keys()) == ORDERS_COLS


class TestDecodeDelete:
    def test_basic_delete(self):
        result = _decode_message(_make_delete_msg(ORDERS_REL_ID, ["5", "Frank", "SSD", "delivered", "2024-01-04"]))
        assert result["tag"] == "delete"
        assert result["new"] is None
        assert result["old"]["id"] == "5"
        assert result["old"]["status"] == "delivered"

    def test_delete_null_field(self):
        result = _decode_message(_make_delete_msg(ORDERS_REL_ID, ["6", None, "Lamp", "pending", "2024-01-05"]))
        assert result["old"]["customer_name"] is None


class TestDecodeIgnoredTypes:
    @pytest.mark.parametrize("msg_byte", [b"B", b"C", b"O", b"Y"])
    def test_ignored_messages_return_none(self, msg_byte):
        assert _decode_message(msg_byte + b"\x00" * 20) is None

    def test_empty_payload(self):
        assert _decode_message(b"") is None


class TestBuildPayload:
    def test_insert_payload(self):
        row = {"id": "1", "status": "pending"}
        payload = _build_payload("INSERT", row)
        assert payload["operation"] == "INSERT"
        assert payload["data"] == row
        assert payload["previous"] is None
        assert "timestamp" in payload

    def test_update_payload(self):
        new = {"id": "1", "status": "shipped"}
        old = {"id": "1", "status": "pending"}
        payload = _build_payload("UPDATE", new, old)
        assert payload["data"] == new
        assert payload["previous"] == old

    def test_delete_uses_old_row_as_data(self):
        old = {"id": "2", "status": "delivered"}
        payload = _build_payload("DELETE", None, old)
        assert payload["data"] == old
        assert payload["previous"] == old

    def test_timestamp_is_iso_format(self):
        from datetime import datetime
        payload = _build_payload("INSERT", {"id": "1"})
        datetime.fromisoformat(payload["timestamp"])
