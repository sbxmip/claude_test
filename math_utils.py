def discount_price(price: float, discount_pct: float) -> float:
    """
    Returns discounted price.
    discount_pct is expected as 0-100.
    Example: 20 means 20%.
    """
    return price * (1 - discount_pct / 100)
