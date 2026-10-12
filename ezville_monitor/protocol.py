"""Wire format for outbound-only telemetry; no HA credentials leave the host."""
import hashlib
import hmac
import json
import re

MAX_BODY = 262144


def redact(text):
    text = re.sub(r'(?i)(bearer\s+)\S+', r'\1[REDACTED]', text)
    text = re.sub(r'''(?ix)((?:password|passwd|token|secret|api_key|mqtt_id|ew11_id)["']?\s*[:=]\s*)["']?[^\s,;}]+''', r'\1[REDACTED]', text)
    text = re.sub(r'(https?://)[^/\s:@]+:[^/\s@]+@', r'\1[REDACTED]@', text)
    text = re.sub(r'\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b', '[REDACTED]', text)
    return text[:4096]


def encode(batch):
    body = json.dumps(batch, ensure_ascii=False, separators=(',', ':')).encode()
    if len(body) > MAX_BODY:
        raise ValueError('telemetry batch too large')
    return body


def signature(secret, timestamp, body):
    return hmac.new(secret.encode(), timestamp.encode() + b'.' + body, hashlib.sha256).hexdigest()


def authenticated(secret, timestamp, body, supplied, now):
    try:
        fresh = abs(now - int(timestamp)) <= 300
    except (ValueError, TypeError):
        return False
    return fresh and hmac.compare_digest(signature(secret, timestamp, body), supplied)
