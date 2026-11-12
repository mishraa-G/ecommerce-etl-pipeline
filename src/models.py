import json
import random
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Dict, Any, Optional

EVENT_TYPES = [
    "product_viewed",
    "cart_item_added",
    "cart_item_removed",
    "checkout_started",
    "order_placed",
    "payment_completed"
]

CURRENCIES = ["USD", "EUR", "GBP"]

@dataclass
class EcommerceEvent:
    event_id: str
    event_type: str
    user_id: str
    product_id: Optional[str]
    order_id: Optional[str]
    quantity: int
    amount: float
    currency: str
    event_timestamp: str  # ISO 8601 string
    metadata: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EcommerceEvent":
        return cls(
            event_id=data["event_id"],
            event_type=data["event_type"],
            user_id=data["user_id"],
            product_id=data.get("product_id"),
            order_id=data.get("order_id"),
            quantity=data.get("quantity", 1),
            amount=float(data.get("amount", 0.0)),
            currency=data.get("currency", "USD"),
            event_timestamp=data["event_timestamp"],
            metadata=data.get("metadata", {})
        )

    @classmethod
    def generate_random(cls, event_id: Optional[str] = None) -> "EcommerceEvent":
        """Generates realistic synthetic e-commerce event stream data."""
        event_type = random.choice(EVENT_TYPES)
        user_id = f"usr_{random.randint(1000, 99999)}"
        product_id = f"prd_{random.randint(100, 5000)}" if event_type != "checkout_started" else None
        
        is_order_event = event_type in ("order_placed", "payment_completed")
        order_id = f"ord_{random.randint(100000, 999999)}" if is_order_event else None
        
        quantity = random.randint(1, 5) if event_type in ("cart_item_added", "order_placed") else 1
        amount = round(random.uniform(5.99, 499.99) * quantity, 2) if is_order_event else 0.0

        return cls(
            event_id=event_id or str(uuid.uuid4()),
            event_type=event_type,
            user_id=user_id,
            product_id=product_id,
            order_id=order_id,
            quantity=quantity,
            amount=amount,
            currency="USD",
            event_timestamp=datetime.now(timezone.utc).isoformat(),
            metadata={
                "ip_address": f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}",
                "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X)",
                "session_id": str(uuid.uuid4())[:8],
                "device": random.choice(["mobile", "desktop", "tablet"])
            }
        )

