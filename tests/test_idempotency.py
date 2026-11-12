import unittest
from src.models import EcommerceEvent
from src.db import DatabaseManager

class TestIdempotentUpsert(unittest.TestCase):
    def setUp(self):
        self.db = DatabaseManager()

    def test_zero_duplicates_on_duplicate_batch_insert(self):
        # 1. Generate 100 unique events
        events = [EcommerceEvent.generate_random() for _ in range(100)]

        # 2. Bulk insert first time
        self.db.bulk_upsert_events(events)
        initial_count = self.db.get_total_count()
        self.assertEqual(initial_count, 100)

        # 3. Simulate replaying the exact same 100 events (e.g. broker retry after timeout)
        self.db.bulk_upsert_events(events)
        replayed_count = self.db.get_total_count()
        
        # 4. Assert total count does not increase and duplicates remain 0
        self.assertEqual(replayed_count, 100)
        self.assertEqual(self.db.get_duplicate_count(), 0)

    def test_partial_overlapping_batches(self):
        batch_a = [EcommerceEvent.generate_random() for _ in range(50)]
        batch_b = [EcommerceEvent.generate_random() for _ in range(50)]

        # Combined batch with overlap
        overlapping_batch = batch_a[25:] + batch_b  # 25 from batch_a (duplicates), 50 from batch_b (new)

        self.db.bulk_upsert_events(batch_a)
        self.assertEqual(self.db.get_total_count(), 50)

        self.db.bulk_upsert_events(overlapping_batch)
        self.assertEqual(self.db.get_total_count(), 100)  # 50 + 50 = 100
        self.assertEqual(self.db.get_duplicate_count(), 0)

if __name__ == "__main__":
    unittest.main()

