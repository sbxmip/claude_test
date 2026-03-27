from math_utils import discount_price

def test_discount_price_20pct():
    assert discount_price(200, 20) == 160

def test_discount_price_25pct():
    assert discount_price(80, 25) == 60

def test_discount_price_10pct():
    assert discount_price(50, 10) == 45
