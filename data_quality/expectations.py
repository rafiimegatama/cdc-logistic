"""
Data Quality Expectations for CDC Logistics Pipeline
Using Great Expectations core (no CLI needed)
"""

# ==========================================
# EXPECTATION DEFINITIONS PER TABLE
# ==========================================

EXPECTATIONS = {

    "orders": [
        # Not null checks
        {"type": "not_null",    "column": "order_id"},
        {"type": "not_null",    "column": "customer_id"},
        {"type": "not_null",    "column": "order_status"},
        {"type": "not_null",    "column": "total_amount"},
        {"type": "not_null",    "column": "payment_method"},
        # Value set checks
        {"type": "in_set",      "column": "order_status",
         "values": ["PENDING", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"]},
        {"type": "in_set",      "column": "payment_method",
         "values": ["TRANSFER", "CREDIT_CARD", "GOPAY", "OVO", "DANA", "COD"]},
        # Range checks
        {"type": "min_value",   "column": "total_amount",   "value": 0},
        {"type": "min_value",   "column": "order_id",       "value": 1},
        {"type": "min_value",   "column": "customer_id",    "value": 1},
    ],

    "order_items": [
        {"type": "not_null",    "column": "item_id"},
        {"type": "not_null",    "column": "order_id"},
        {"type": "not_null",    "column": "product_id"},
        {"type": "not_null",    "column": "quantity"},
        {"type": "not_null",    "column": "unit_price"},
        {"type": "not_null",    "column": "subtotal"},
        {"type": "min_value",   "column": "quantity",       "value": 1},
        {"type": "min_value",   "column": "unit_price",     "value": 0},
        {"type": "min_value",   "column": "subtotal",       "value": 0},
    ],

    "shipments": [
        {"type": "not_null",    "column": "shipment_id"},
        {"type": "not_null",    "column": "order_id"},
        {"type": "not_null",    "column": "courier"},
        {"type": "not_null",    "column": "tracking_number"},
        {"type": "not_null",    "column": "shipment_status"},
        {"type": "in_set",      "column": "courier",
         "values": ["JNE", "SiCepat", "JNT", "Anteraja", "TIKI", "Pos Indonesia"]},
        {"type": "in_set",      "column": "shipment_status",
         "values": ["WAITING_PICKUP", "IN_TRANSIT", "OUT_FOR_DELIVERY",
                    "DELIVERED", "FAILED", "PICKUP"]},
        {"type": "min_value",   "column": "shipment_id",    "value": 1},
    ],

    "delivery_events": [
        {"type": "not_null",    "column": "event_id"},
        {"type": "not_null",    "column": "shipment_id"},
        {"type": "not_null",    "column": "event_type"},
        {"type": "not_null",    "column": "event_location"},
        {"type": "in_set",      "column": "event_type",
         "values": ["PICKUP", "IN_TRANSIT", "OUT_FOR_DELIVERY",
                    "DELIVERED", "FAILED"]},
        {"type": "min_value",   "column": "event_id",       "value": 1},
        {"type": "min_value",   "column": "shipment_id",    "value": 1},
    ]
}

# ==========================================
# VALIDATOR
# ==========================================
def validate(data: dict, table_id: str) -> tuple[bool, list, list]:
    """
    Validate a data dict against expectation suite.
    Returns (is_valid, errors, warnings)
    """
    expectations = EXPECTATIONS.get(table_id, [])
    errors   = []
    warnings = []

    for exp in expectations:
        col   = exp["column"]
        etype = exp["type"]
        value = data.get(col)

        try:
            if etype == "not_null":
                if value is None:
                    errors.append(f"[{etype}] Column '{col}' is NULL")

            elif etype == "in_set":
                allowed = exp["values"]
                if value is not None and value not in allowed:
                    errors.append(
                        f"[{etype}] Column '{col}' value '{value}' "
                        f"not in allowed set {allowed}"
                    )

            elif etype == "min_value":
                min_val = exp["value"]
                if value is not None and value < min_val:
                    errors.append(
                        f"[{etype}] Column '{col}' value {value} "
                        f"is below minimum {min_val}"
                    )

            elif etype == "max_value":
                max_val = exp["value"]
                if value is not None and value > max_val:
                    errors.append(
                        f"[{etype}] Column '{col}' value {value} "
                        f"exceeds maximum {max_val}"
                    )

            elif etype == "regex":
                import re
                pattern = exp["pattern"]
                if value is not None and not re.match(pattern, str(value)):
                    warnings.append(
                        f"[{etype}] Column '{col}' value '{value}' "
                        f"does not match pattern '{pattern}'"
                    )

        except Exception as e:
            warnings.append(f"[{etype}] Validation error on '{col}': {e}")

    is_valid = len(errors) == 0
    return is_valid, errors, warnings


# ==========================================
# VALIDATION REPORT
# ==========================================
def validation_report(
    data: dict,
    table_id: str,
    operation: str
) -> dict:
    """Generate a full validation report for a record."""
    is_valid, errors, warnings = validate(data, table_id)

    return {
        "table":     table_id,
        "operation": operation,
        "is_valid":  is_valid,
        "errors":    errors,
        "warnings":  warnings,
        "record_id": data.get(f"{table_id[:-1]}_id") or data.get("event_id"),
    }
