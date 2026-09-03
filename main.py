from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import Response
import httpx, json, os, asyncio
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager

app = FastAPI()
QUEUE_FILE = "/tmp/whatsapp_queue.json"
WHATSAPP_TOKEN="EAAOzn...ZDZD"
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
    """Meta webhook verification - GET endpoint"""
    mode = request.query_params.get("hub.mode")
    token = request.query_params.get("hub.verify_token")
    challenge = request.query_params.get("hub.challenge")

    if mode == "subscribe" and token == VERIFY_TOKEN:
        return Response(content=challenge, status_code=200)
    return Response(content="Forbidden", status_code=403)

@app.post("/webhook")
async def webhook(request: Request):
    """Receives messages from Cloudflare Worker"""
    data = await request.json()
    queue = load_queue()

    # Handle new message from Worker
    message = data.get("message")
    if not message:
        return {"status": "no message"}

    msg_id = message.get("message_id")
    sender = message.get("sender")
    text = message.get("text")
    phone_number_id = message.get("phone_number_id", PHONE_NUMBER_ID)

    # Check if message already exists
    existing = next((m for m in queue if m.get("message_id") == msg_id), None)
    if existing:
        return {"status": "already_queued"}

    # Check if this is a NEW customer (first time sender)
    is_new_customer = not any(m.get("sender") == sender for m in queue)

    queue.append({
        "id": len(queue) + 1,
        "message_id": msg_id,
        "sender": sender,
        "text": text,
        "phone_number_id": phone_number_id,
        "message_type": message.get("message_type", "text"),
        "status": "pending",
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
    """Hermes calls this to get next pending message"""
    queue = load_queue()
    for msg in queue:
        if msg.get("status") == "pending":
            msg["status"] = "processing"
            save_queue(queue)
            return {"message": msg}
    return {"message": None}

@app.post("/reply")
async def reply(request: Request):
    """Hermes sends reply here, FastAPI delivers to WhatsApp"""
    data = await request.json()
    msg_id = data.get("id")
    reply_text = data.get("reply", "").strip()

    if not msg_id or not reply_text:
        raise HTTPException(status_code=400, detail="id and reply required")

    queue = load_queue()
    msg = next((m for m in queue if m.get("id") == msg_id), None)

    if not msg:
        raise HTTPException(status_code=404, detail="message not found")

    # Send reply to WhatsApp
    await send_wa(
        msg["sender"],
        reply_text,
        msg.get("phone_number_id", PHONE_NUMBER_ID),
        reply_to=msg.get("message_id")
    )

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
    """Get queue statistics"""
    queue = load_queue()
    total = len(queue)
    pending = sum(1 for m in queue if m.get("status") == "pending")
    processing = sum(1 for m in queue if m.get("status") == "processing")
    completed = sum(1 for m in queue if m.get("status") == "completed")
    return {
        "total": total,
        "pending": pending,
        "processing": processing,
        "completed": completed
    }
