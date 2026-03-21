import pytest
import pandas as pd
import numpy as np

from src.ingestion.silver_writer import (
    _remove_special_characters,
    _normalize_partition_name,
    _transform_country_state,
    get_australia_state_mapping,
)

class TestRemoveSpecialCharacters:

    def test_removes_accents(self):
        assert _remove_special_characters("Kärnten") == "Karnten"

    def test_empty_string_returns_unknown(self):
        assert _remove_special_characters("") == "unknown"

    def test_string_with_only_symbols_returns_unknown(self):
        assert _remove_special_characters("@#$!") == "unknown"

    def test_keeps_hyphens(self):
        assert _remove_special_characters("Rhône-Alpes") == "Rhone-Alpes"

class TestNormalizePartitionName:

    def test_lowercases_text(self):
        assert _normalize_partition_name("California") == "california"

    def test_replaces_spaces_with_hyphens(self):
        assert _normalize_partition_name("New York") == "new-york"

    def test_strips_whitespace(self):
        assert _normalize_partition_name("  Texas  ") == "texas"

    def test_removes_accents_and_lowercases(self):
        assert _normalize_partition_name("São Paulo") == "sao-paulo"

    def test_empty_string_returns_unknown(self):
        assert _normalize_partition_name("") == "unknown"

class TestTransformCountryState:

    def _make_df(self, rows: list[dict]) -> pd.DataFrame:
        return pd.DataFrame(rows)

    def test_strips_whitespace_from_country_and_state(self):

        df = self._make_df([{"country": "  United States  ", "state": "  California  "}])

        result = _transform_country_state(df)

        assert result["country"].iloc[0] == "United States"
        assert result["state"].iloc[0] == "California"

    def test_expands_all_australian_abbreviations(self):

        abbreviations = get_australia_state_mapping()

        for abbr, full_name in abbreviations.items():
            df = self._make_df([{"country": "Australia", "state": abbr}])

            result = _transform_country_state(df)

            assert result["state"].iloc[0] == full_name, f"Failed for {abbr}"

    def test_does_not_expand_abbreviation_for_non_australia(self):

        df = self._make_df([{"country": "United States", "state": "VIC"}])

        result = _transform_country_state(df)

        # Should stay as-is (after removing special chars), not be expanded
        assert result["state"].iloc[0] == "VIC"

    def test_removes_accents_from_state(self):

        df = self._make_df([{"country": "Brazil", "state": "São Paulo"}])

        result = _transform_country_state(df)

        assert result["state"].iloc[0] == "Sao Paulo"