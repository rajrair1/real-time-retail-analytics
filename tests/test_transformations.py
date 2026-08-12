from decimal import Decimal
import pytest
from src.transformations import calculate_net_amount, is_valid_event

def test_net_amount():
    assert calculate_net_amount(2, 10.00, 0.10) == Decimal("18.00")

def test_invalid_quantity():
    with pytest.raises(ValueError):
        calculate_net_amount(0, 10, 0)

def test_required_fields():
    assert not is_valid_event({"event_id": "x"})
