"""
Unit tests for CSV parsing, validation, and service helpers.
No network calls are made here.
"""
from __future__ import annotations

import pytest

from app.services import MAX_HOSPITALS, parse_csv, validate_csv


# --------------------------------------------------------------------------- #
# parse_csv                                                                     #
# --------------------------------------------------------------------------- #


class TestParseCsv:
    def _csv(self, *lines: str) -> bytes:
        return "\n".join(lines).encode()

    def test_valid_with_phone(self):
        data = self._csv(
            "name,address,phone",
            "City Hospital,123 Main St,555-1234",
        )
        hospitals, errors = parse_csv(data)
        assert len(hospitals) == 1
        assert errors == []
        h = hospitals[0]
        assert h.name == "City Hospital"
        assert h.address == "123 Main St"
        assert h.phone == "555-1234"
        assert h.row == 1

    def test_valid_without_phone(self):
        data = self._csv(
            "name,address",
            "Metro Clinic,456 Elm Ave",
        )
        hospitals, errors = parse_csv(data)
        assert len(hospitals) == 1
        assert hospitals[0].phone is None

    def test_multiple_rows(self):
        data = self._csv(
            "name,address,phone",
            "Hospital A,1 A St,111",
            "Hospital B,2 B St,222",
            "Hospital C,3 C St,",
        )
        hospitals, errors = parse_csv(data)
        assert len(hospitals) == 3
        assert errors == []

    def test_missing_required_column_raises(self):
        data = self._csv("name,phone", "City Hospital,555")
        with pytest.raises(ValueError, match="address"):
            parse_csv(data)

    def test_empty_file_raises(self):
        with pytest.raises(ValueError):
            parse_csv(b"")

    def test_row_with_empty_name_skipped(self):
        data = self._csv(
            "name,address",
            ",456 Elm Ave",
            "Good Hospital,789 Oak Rd",
        )
        hospitals, errors = parse_csv(data)
        assert len(hospitals) == 1
        assert len(errors) == 1
        assert "Row 1" in errors[0]

    def test_row_with_empty_address_skipped(self):
        data = self._csv(
            "name,address",
            "Some Hospital,",
        )
        hospitals, errors = parse_csv(data)
        assert len(hospitals) == 0
        assert len(errors) == 1

    def test_bom_header_stripped(self):
        """CSV files from Excel often have a UTF-8 BOM."""
        data = ("name,address\nBOM Hospital,1 BOM St").encode("utf-8-sig")
        hospitals, errors = parse_csv(data)
        assert len(hospitals) == 1
        assert errors == []

    def test_extra_columns_ignored(self):
        data = self._csv(
            "name,address,phone,rating",
            "Hospital X,1 X St,999,5",
        )
        hospitals, errors = parse_csv(data)
        assert len(hospitals) == 1

    def test_row_numbers_are_1_indexed(self):
        data = self._csv(
            "name,address",
            "First,Addr 1",
            "Second,Addr 2",
        )
        hospitals, _ = parse_csv(data)
        assert hospitals[0].row == 1
        assert hospitals[1].row == 2


# --------------------------------------------------------------------------- #
# validate_csv                                                                  #
# --------------------------------------------------------------------------- #


class TestValidateCsv:
    def _csv(self, *lines: str) -> bytes:
        return "\n".join(lines).encode()

    def test_valid_csv_passes(self):
        data = self._csv(
            "name,address",
            "Hospital A,Street 1",
            "Hospital B,Street 2",
        )
        result = validate_csv(data)
        assert result.valid is True
        assert result.errors == []
        assert result.total_rows == 2

    def test_exceeds_max_hospitals(self):
        rows = ["name,address"] + [f"H{i},Addr {i}" for i in range(MAX_HOSPITALS + 1)]
        data = self._csv(*rows)
        result = validate_csv(data)
        assert result.valid is False
        assert any("maximum" in e for e in result.errors)

    def test_preview_limited_to_5(self):
        rows = ["name,address"] + [f"H{i},Addr {i}" for i in range(10)]
        data = self._csv(*rows)
        result = validate_csv(data)
        assert len(result.preview) == 5

    def test_unknown_columns_produce_warning(self):
        data = self._csv(
            "name,address,fax",
            "Clinic,Road 1,000",
        )
        result = validate_csv(data)
        assert result.valid is True
        assert any("fax" in w for w in result.warnings)

    def test_invalid_csv_fails(self):
        result = validate_csv(b"")
        assert result.valid is False
        assert len(result.errors) > 0
