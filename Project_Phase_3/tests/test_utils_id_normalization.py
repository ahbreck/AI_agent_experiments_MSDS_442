import sqlite3
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
PHASE3_ROOT = REPO_ROOT / "Project_Phase_3"
if str(PHASE3_ROOT) not in sys.path:
    sys.path.insert(0, str(PHASE3_ROOT))

from prototype.utils import (  # noqa: E402
    extract_explicit_member_id,
    member_id_aliases,
    normalize_campaign_id,
    normalize_id,
    normalize_member_id,
    register_sqlite_alnum_normalizer,
)


class TestIdNormalization(unittest.TestCase):
    def test_normalize_id_strips_non_alnum(self):
        self.assertEqual(normalize_id(" mb-00_1# "), "MB001")
        self.assertEqual(normalize_id("camp-10_2"), "CAMP102")

    def test_member_id_and_campaign_id_normalization(self):
        self.assertEqual(normalize_member_id("mb-001"), "MB001")
        self.assertEqual(normalize_member_id("M_001"), "MB001")
        self.assertEqual(normalize_campaign_id("camp#204"), "CAMP204")

    def test_extract_member_id_tolerates_non_alnum_separator(self):
        self.assertEqual(extract_explicit_member_id("my member id is MB#001"), "MB001")
        self.assertEqual(extract_explicit_member_id("id: M-001"), "MB001")

    def test_member_aliases_are_alnum(self):
        self.assertEqual(member_id_aliases("MB-001"), ["MB001", "M001", "001"])

    def test_sqlite_normalizer_function(self):
        conn = sqlite3.connect(":memory:")
        register_sqlite_alnum_normalizer(conn)
        got = conn.execute("SELECT NORM_ALNUM('MB-00_1#')").fetchone()[0]
        self.assertEqual(got, "MB001")
        conn.close()


if __name__ == "__main__":
    unittest.main()
