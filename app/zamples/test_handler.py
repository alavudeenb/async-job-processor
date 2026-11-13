from app.worker.handler_resolver import resolve_handler

# handler = resolve_handler("generate_report")

# result = handler({"report_id": "RPT-001"})

handler = resolve_handler("send_email")
result = handler({"recipient": "abc@xys.com"})

print(result)
