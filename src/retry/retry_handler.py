"""
FastAPI HTTP endpoint that receives Cloud Tasks callbacks for retried messages.

Cloud Tasks calls POST /retry with the original message payload and attempt
number. We re-run the same consumer pipeline logic from a fresh context.
"""

import json
import logging
import os

from fastapi import FastAPI, HTTPException, Request, Response

from ..schema.er_record import ERRecord
from ..storage.mongo_writer import MongoWriter

logger = logging.getLogger(__name__)

app = FastAPI(title="ER-Insight Retry Handler")

_writer: MongoWriter | None = None


def get_writer() -> MongoWriter:
    global _writer
    if _writer is None:
        _writer = MongoWriter(
            uri=os.environ["MONGO_URI"],
            db_name=os.environ.get("MONGO_DB", "er_insight"),
        )
    return _writer


@app.post("/retry")
async def handle_retry(request: Request):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    message_id = body.get("message_id")
    payload = body.get("payload")
    attempt = body.get("attempt", 1)

    if not message_id or payload is None:
        raise HTTPException(status_code=400, detail="Missing message_id or payload")

    logger.info("Cloud Tasks retry: message_id=%s attempt=%d", message_id, attempt)

    try:
        record = ERRecord.from_dict(payload)
        get_writer().write(record.to_mongo())
        get_writer().flush()
        logger.info("Retry succeeded: %s", message_id)
        return Response(status_code=200)
    except Exception as e:
        logger.error("Retry failed for %s (attempt %d): %s", message_id, attempt, e)
        # Return 5xx so Cloud Tasks knows to retry (up to its own retry config)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/healthz")
async def health():
    return {"status": "ok"}
