from typing import Any


def validate_row(row: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    """
    Validate and clean a single order row.
    Returns (cleaned_row, None) if valid, or (None, reason) if rejected.
    """
    raw_order_id = str(row.get("order_id", "")).strip()
    if not raw_order_id:
        return None, "missing_order_id"
    try:
        if "." in raw_order_id:
            return None, "invalid_order_id"
        order_id = int(raw_order_id)
    except ValueError:
        return None, "invalid_order_id"

    customer_id = str(row.get("customer_id", "")).strip()
    if not customer_id:
        return None, "empty_customer_id"

    raw_amount = str(row.get("amount", "")).strip()
    try:
        amount = float(raw_amount)
    except ValueError:
        return None, "invalid_amount"
    if amount <= 0:
        return None, "non_positive_amount"

    status = str(row.get("status", "")).strip().upper()
    order_date = str(row.get("order_date", "")).strip()

    cleaned = {
        "order_id": order_id,
        "customer_id": customer_id,
        "order_date": order_date,
        "amount": round(amount, 2),
        "status": status,
    }
    return cleaned, None