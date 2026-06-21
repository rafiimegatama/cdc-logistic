import json
import logging
import os
import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

SCHEMA_FILES = {
    "cdc-orders-value":       "schemas/orders.avsc",
    "cdc-order-items-value":  "schemas/order_items.avsc",
    "cdc-shipments-value":    "schemas/shipments.avsc",
    "cdc-delivery-value":     "schemas/delivery_events.avsc"
}

# ==========================================
# REGISTER SCHEMA
# ==========================================
def register_schema(subject: str, schema_path: str) -> int:
    with open(schema_path, "r") as f:
        schema_str = json.dumps(json.load(f))

    payload = {"schema": schema_str}
    url     = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions"
    resp    = requests.post(
        url, json=payload,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"}
    )

    if resp.status_code in (200, 201):
        schema_id = resp.json().get("id")
        log.info(f"✅ Registered schema | subject={subject} | id={schema_id}")
        return schema_id
    else:
        log.error(f"❌ Failed to register | subject={subject} | {resp.text}")
        raise Exception(f"Schema registration failed: {resp.text}")

# ==========================================
# VALIDATE MESSAGE AGAINST SCHEMA
# ==========================================
def validate_message(data: dict, subject: str) -> tuple[bool, list]:
    """
    Validate a message dict against registered Avro schema.
    Returns (is_valid, list_of_errors)
    """
    errors = []

    try:
        url  = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest"
        resp = requests.get(url, timeout=5)

        if resp.status_code != 200:
            # If schema not found, allow message through
            log.warning(f"⚠️ Schema not found for {subject}, skipping validation")
            return True, []

        schema = json.loads(resp.json()["schema"])
        fields = {f["name"]: f for f in schema.get("fields", [])}

        # Check for unexpected fields (ignore CDC added fields)
        cdc_fields = {"cdc_operation", "cdc_timestamp"}
        for key in data:
            if key not in fields and key not in cdc_fields:
                errors.append(f"Unexpected field: '{key}'")

        # Validate field types using simple type mapping
        for field_name, field_def in fields.items():
            if field_name not in data:
                continue

            value       = data[field_name]
            field_types = field_def.get("type", [])

            # Normalize to list — handle both string and list types
            if isinstance(field_types, str):
                allowed = [field_types]
            elif isinstance(field_types, list):
                allowed = field_types
            else:
                continue

            # Flatten union types (e.g. ["null", "string"] → ["null", "string"])
            py_types = []
            for t in allowed:
                if isinstance(t, str):
                    if t == "null":
                        py_types.append(type(None))
                    elif t in ("int", "long"):
                        py_types.append(int)
                    elif t == "double":
                        py_types.extend([float, int])
                    elif t == "string":
                        py_types.append(str)
                    elif t == "boolean":
                        py_types.append(bool)
                elif isinstance(t, dict):
                    # Complex types like {"type": "record"} — skip deep validation
                    py_types.append(object)

            if py_types and value is not None:
                if not isinstance(value, tuple(py_types)):
                    errors.append(
                        f"Field '{field_name}': "
                        f"expected {allowed}, got {type(value).__name__} "
                        f"(value={value})"
                    )

    except requests.exceptions.ConnectionError:
        log.warning("⚠️ Schema Registry unreachable, skipping validation")
        return True, []
    except Exception as e:
        log.warning(f"⚠️ Validation skipped due to error: {e}")
        return True, []  # Fail open — don't block pipeline on validator errors

    is_valid = len(errors) == 0
    return is_valid, errors

# ==========================================
# HEALTH CHECK
# ==========================================
def check_health() -> bool:
    try:
        resp = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects", timeout=5)
        if resp.status_code == 200:
            log.info(f"✅ Schema Registry healthy | subjects={resp.json()}")
            return True
        return False
    except Exception as e:
        log.error(f"❌ Schema Registry unreachable: {e}")
        return False

# ==========================================
# REGISTER ALL SCHEMAS
# ==========================================
def register_all():
    log.info("🚀 Registering all schemas...")
    results = {}
    for subject, path in SCHEMA_FILES.items():
        try:
            schema_id        = register_schema(subject, path)
            results[subject] = {"status": "ok", "id": schema_id}
        except Exception as e:
            results[subject] = {"status": "error", "error": str(e)}
    return results

# ==========================================
# LIST ALL SCHEMAS
# ==========================================
def list_schemas():
    resp = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects")
    if resp.status_code == 200:
        subjects = resp.json()
        log.info(f"📋 Registered subjects: {subjects}")
        return subjects
    return []

if __name__ == "__main__":
    if check_health():
        register_all()
        list_schemas()
    else:
        log.error("❌ Schema Registry not available")
