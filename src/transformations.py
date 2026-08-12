from decimal import Decimal


def calculate_net_amount(quantity: int, unit_price: float, discount_rate: float) -> Decimal:
    if quantity <= 0 or unit_price < 0 or not 0 <= discount_rate <= 1:
        raise ValueError("invalid transaction values")
    return (Decimal(str(quantity)) * Decimal(str(unit_price)) * (1 - Decimal(str(discount_rate)))).quantize(Decimal("0.01"))


def is_valid_event(event: dict) -> bool:
    required = {"event_id", "event_time", "product_id", "store_id", "quantity", "unit_price", "discount_rate"}
    return required.issubset(event) and event["quantity"] > 0 and event["unit_price"] >= 0 and 0 <= event["discount_rate"] <= 1
