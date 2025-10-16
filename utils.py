from decimal import Decimal

def format_price(price) -> str:
    """4 знака после запятой, русский разделитель"""
    return f"{float(price):.4f}".replace(".", ",")
