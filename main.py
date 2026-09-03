from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import Response
import httpx, json, os, asyncio
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager

app = FastAPI()
QUEUE_FILE = "/app/data/whatsapp_queue.json"
WHATSAPP_TOKEN="EAAOzna8UP9sBSayVzrLAE3N1a58eA6cZBZAbdkwZCszFbfNZCUq6I12ZB3Tj1TRBHgkIpOWoHUZAHxkoE46FKS1nrLaAjBNIbtcXbSik7ZAiT5BeZAVtHU6v1I1ZCY7d9AyrIfih0DfgvQegv6ekpx40L4N2dgnQ7ZA1slFKDiD3BriiYqVOZCIEhlwZBF3RuymqA6oN9kZBaOJeHpd0OdX8pEQuBo2dS7JZBrdFSmVuIRMOnV6iWX1MBy"
PHONE_NUMBER_ID = "1203156892891204"
VERIFY_TOKEN="luka_verify_2026"
EGYPT_TZ = timezone(timedelta(hours=3))

os.makedirs(os.path.dirname(QUEUE_FILE), exist_ok=True)

def load_queue():
    if os.path.exists(QUEUE_FILE):
        with open(QUEUE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []

def save_queue(queue):
    with open(QUEUE_FILE, "w", encoding="utf-8") as f:
        json.dump(queue, f, ensure_ascii=False, indent=2)

async def send_wa(to, text, pid, reply_to=None):
    if not to or not text:
        return
    url = f"https://graph.facebook.com/v18.0/{pid}/messages"
    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json; charset=utf-8"
    }
    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "text",
        "text": {"body": text}
    }
    if reply_to:
        payload["context"] = {"message_id": reply_to}
    async with httpx.AsyncClient() as client:
        await client.post(url, json=payload, headers=headers)

@app.get("/webhook")
async def verify_webhook(request: Request):
    mode = request.query_params.get("hub.mode")
    token = request.query_params.get("hub.verify_token")
    challenge = request.query_params.get("hub.challenge")
    if mode == "subscribe" and token == VERIFY_TOKEN:
        return Response(content=challenge, status_code=200)
    return Response(content="Forbidden", status_code=403)

@app.post("/webhook")
async def webhook(request: Request):
    data = await request.json()
    queue = load_queue()

    # Handle status updates from Meta
    try:
        value = data["entry"][0]["changes"][0]["value"]
        if "statuses" in value:
            for status in value["statuses"]:
                msg_id = status.get("id")
                msg_status = status.get("status")
                for msg in queue:
                    if msg.get("message_id") == msg_id:
                        msg["status"] = msg_status
                        msg["status_updated_at"] = datetime.now(EGYPT_TZ).isoformat()
                        break
            save_queue(queue)
            return {"status": "updated"}
    except (KeyError, IndexError):
        pass

    # Handle new message from Meta
    try:
        value = data["entry"][0]["changes"][0]["value"]
        message = value["messages"][0]
    except (KeyError, IndexError):
        return {"status": "no message"}

    msg_id = message.get("id")
    sender = message.get("from")
    text = message.get("text", {}).get("body", "")
    phone_number_id = value.get("metadata", {}).get("phone_number_id", PHONE_NUMBER_ID)

    existing = next((m for m in queue if m.get("message_id") == msg_id), None)
    if existing:
        return {"status": "already_queued"}

    is_new_customer = not any(m.get("sender") == sender for m in queue)

    queue.append({
        "id": len(queue) + 1,
        "message_id": msg_id,
        "sender": sender,
        "text": text,
        "phone_number_id": phone_number_id,
        "message_type": "text",
        "status": "sent",
        "processed": False,
        "reply": "",
        "whatsapp_message_id": msg_id,
        "timestamp": datetime.now(EGYPT_TZ).isoformat()
    })

    save_queue(queue)

    # Send read receipt
    await send_wa(sender, "", phone_number_id, reply_to=msg_id)

    # Fallback after 8 seconds ONLY for NEW customers
    if is_new_customer:
        async def fallback():
            await asyncio.sleep(8)
            current_queue = load_queue()
            for msg in current_queue:
                if msg.get("message_id") == msg_id and not msg.get("processed"):
                    fallback_text = "تم استلام رسالتك بنجاح، هيتواصل معاك فريق لوكا تيك فى أقرب وقت."
                    await send_wa(msg["sender"], fallback_text, msg.get("phone_number_id", PHONE_NUMBER_ID), reply_to=msg.get("message_id"))
                    msg["processed"] = True
                    msg["reply"] = fallback_text
                    msg["sent_to_whatsapp"] = True
                    msg["status"] = "completed"
                    msg["processed_at"] = datetime.now(EGYPT_TZ).isoformat()
                    msg["sent_at"] = datetime.now(EGYPT_TZ).isoformat()
                    save_queue(current_queue)
                    break
        asyncio.create_task(fallback())

    return {"status": "queued"}

@app.get("/pull")
async def pull():
    queue = load_queue()
    for msg in queue:
        if msg.get("status") == "pending":
            msg["status"] = "processing"
            save_queue(queue)
            return {"message": msg}
    return {"message": None}

@app.post("/reply")
async def reply(request: Request):
    data = await request.json()
    msg_id = data.get("id")
    reply_text = data.get("reply", "").strip()
    if not msg_id or not reply_text:
        raise HTTPException(status_code=400, detail="id and reply required")
    queue = load_queue()
    msg = next((m for m in queue if m.get("id") == msg_id), None)
    if not msg:
        raise HTTPException(status_code=404, detail="message not found")
    await send_wa(msg["sender"], reply_text, msg.get("phone_number_id", PHONE_NUMBER_ID), reply_to=msg.get("message_id"))
    msg["reply"] = reply_text
    msg["processed"] = True
    msg["sent_to_whatsapp"] = True
    msg["status"] = "completed"
    msg["processed_at"] = datetime.now(EGYPT_TZ).isoformat()
    msg["sent_at"] = datetime.now(EGYPT_TZ).isoformat()
    save_queue(queue)
    return {"ok": True}

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/stats")
async def stats():
    queue = load_queue()
    total = len(queue)
    pending = sum(1 for m in queue if m.get("status") == "pending")
    processing = sum(1 for m in queue if m.get("status") == "processing")
    completed = sum(1 for m in queue if m.get("status") == "completed")
    sent = sum(1 for m in queue if m.get("status") == "sent")
    delivered = sum(1 for m in queue if m.get("status") == "delivered")
    read = sum(1 for m in queue if m.get("status") == "read")
    failed = sum(1 for m in queue if m.get("status") == "failed")
    return {"total": total, "pending": pending, "processing": processing, "completed": completed, "sent": sent, "delivered": delivered, "read": read, "failed": failed}
