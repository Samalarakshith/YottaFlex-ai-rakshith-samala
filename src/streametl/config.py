REQUIRED_COLUMNS = ("event_id", "order_id", "customer_id", "event_type", "amount", "currency", "event_timestamp")
ALLOWED_EVENT_TYPES = {"created", "paid", "shipped", "cancelled"}
SUPPORTED_CURRENCIES = {"USD"}
OUTPUT_COLUMNS = ["event_id", "order_id", "customer_id", "event_type", "amount", "currency", "event_timestamp", "event_date", "source_file", "source_row", "first_run_id"]
