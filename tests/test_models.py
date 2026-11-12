import unittest
import json
from src.models import EcommerceEvent

class TestEcommerceEventModel(unittest.TestCase):
    def test_generate_random_event(self):
        event = EcommerceEvent.generate_random()
        self.assertIsNotNone(event.event_id)
        self.assertIn(event.event_type, [
            "product_viewed", "cart_item_added", "cart_item_removed",
            "checkout_started", "order_placed", "payment_completed"
        ])
        self.assertTrue(event.user_id.startswith("usr_"))
        self.assertGreaterEqual(event.amount, 0.0)
        self.assertIn("session_id", event.metadata)

    def test_json_serialization_deserialization(self):
        original = EcommerceEvent.generate_random()
        json_str = original.to_json()
        
        parsed_dict = json.loads(json_str)
        reconstructed = EcommerceEvent.from_dict(parsed_dict)

        self.assertEqual(original.event_id, reconstructed.event_id)
        self.assertEqual(original.event_type, reconstructed.event_type)
        self.assertEqual(original.user_id, reconstructed.user_id)
        self.assertEqual(original.amount, reconstructed.amount)
        self.assertEqual(original.metadata, reconstructed.metadata)

if __name__ == "__main__":
    unittest.main()

