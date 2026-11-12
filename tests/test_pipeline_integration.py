import unittest
from src.models import EcommerceEvent
from src.db import DatabaseManager
from src.consumer import EventConsumer

class TestPipelineIntegration(unittest.TestCase):
    def setUp(self):
        self.db = DatabaseManager()
        self.consumer = EventConsumer(db_manager=self.db)

    def test_consumer_batch_processing_and_offset_sequence(self):
        batch = [EcommerceEvent.generate_random() for _ in range(500)]
        
        # Verify batch processing
        ingested = self.consumer.process_batch(batch)
        self.assertEqual(ingested, 500)
        self.assertEqual(self.consumer.total_ingested, 500)
        self.assertEqual(self.consumer.total_batches, 1)

        # Verify manual commit does not crash
        self.consumer.commit_offset(asynchronous=False)

        # Database state verification
        self.assertEqual(self.db.get_total_count(), 500)
        self.assertEqual(self.db.get_duplicate_count(), 0)

if __name__ == "__main__":
    unittest.main()

