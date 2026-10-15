"""Broker service for CECS 327 Milestone 2 (publish-subscribe). Python 3.10+.

WHAT A BROKER DOES (the idea)
    A *publisher* (for example Post) tells the broker "something happened".
    *Subscribers* (for example Feed) told the broker earlier "I care about
    this kind of event, send it to this URL". The publisher never learns who
    the subscribers are; the broker looks them up and delivers the event.

INSTALL AND START
    python -m pip install flask requests
    python broker_server.py                 # HTTP on 127.0.0.1:5005
    # Start the broker BEFORE Feed: Feed subscribes once, at startup.
    python feed_server.py --broker-url http://127.0.0.1:5005

ENDPOINTS (all bodies are JSON; Content-Type: application/json)
    POST /subscribe     {"topic": "post.created", "url": "http://127.0.0.1:5003/events"}
                        -> 200 {"success": true, "duplicate": false, ...}
                        Registering the same topic+url twice is harmless
                        (duplicate=true, still one registration).
    POST /unsubscribe   same body -> {"success": true, "removed": true|false}
    POST /publish       the event body below -> 200 delivery report (see below)
    GET  /subscriptions -> the current registrations (handy for the demo)
    GET  /health        -> process check only

EVENT BODY (exactly what Feed's POST /events expects)
    {"event_id": "unique-event-001", "type": "post.created",
     "post": {"id": 1, "author": 7, "timestamp": "2026-10-13T02:00:00+00:00",
              "content": "photo.jpg", "caption": "Hi", "likes": [], "comments": []}}
    The broker forwards the event UNCHANGED, so a retry carries the same
    event_id and the same data. Matching is exact: event["type"] must equal
    the subscription topic (no wildcards).

DELIVERY, ACKNOWLEDGMENT AND RETRY (what is and is not guaranteed)
    * Every matching subscriber gets its own HTTP POST, sent in parallel.
      One slow, dead, or failing subscriber cannot stop the others.
    * A delivery counts as ACKNOWLEDGED only when the subscriber answers
      HTTP 2xx with JSON containing "success": true (Feed does this).
    * Timeouts, connection errors, HTTP 5xx, 408 and 429 are retried, up to
      --attempts tries per subscriber (default 3) with exponential backoff
      (--backoff 0.5 -> waits 0.5s, then 1s). Every retry re-sends the same
      event_id and same data, so subscribers that de-duplicate (Feed does) stay
      correct. Other 4xx answers (e.g. 400 for an event_id reused with
      different data) are permanent and are NOT retried.
    * POST /publish answers only after all deliveries finished or gave up, so
      the publisher sees an individual result per subscriber. Worst case time is
      about attempts*timeout + backoff waits (default ~6s + 1.5s).
      success=true means "event valid, delivery attempted"; look at
      "all_delivered", "delivered", "failed" and "results" for the outcome.
    * This is at-least-once effort, NOT durable: the broker keeps no queue.
      If a subscriber is still down after the last attempt, the event is not
      redelivered later. The publisher may re-publish the same event_id.
    * Subscriptions live in memory. If the broker restarts they are lost, and
      Feed must be restarted to subscribe again.
    * No authentication; any caller can subscribe a URL. Local teaching
      prototype only.

LOGS
    UTC timestamps in logs/broker.log and the terminal, with request IDs and the
    event BEFORE/AFTER marshalling and unmarshalling. X-Request-ID is accepted
    on /publish and forwarded to subscribers. event_id is a different thing: it
    identifies the EVENT, while the request ID identifies one HTTP request chain.
"""

import argparse
import json
import logging
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import requests
from flask import Flask, g, jsonify, request


# ---------------------------------------------------------------- logging
def make_logger(name, log_dir="logs"):
    """UTC timestamps to both a file and the terminal (same style as Feed)."""
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if not logger.handlers:
        formatter = logging.Formatter(
            "%(asctime)sZ [%(name)s] %(message)s", "%Y-%m-%dT%H:%M:%S"
        )
        formatter.converter = time.gmtime
        for handler in (logging.StreamHandler(),
                        logging.FileHandler(Path(log_dir) / f"{name}.log")):
            handler.setFormatter(formatter)
            logger.addHandler(handler)
    return logger


def marshal(message, logger, label):
    """Python dictionary -> JSON text -> UTF-8 bytes, logged before and after."""
    logger.info("%s BEFORE_MARSHAL %r", label, message)
    wire = json.dumps(message, ensure_ascii=False, allow_nan=False).encode("utf-8")
    logger.info("%s AFTER_MARSHAL %r", label, wire)
    return wire


def unmarshal(wire, logger, label):
    """UTF-8 bytes -> JSON text -> Python data, logged before and after."""
    logger.info("%s BEFORE_UNMARSHAL %r", label, wire)
    message = json.loads(wire.decode("utf-8"))
    logger.info("%s AFTER_UNMARSHAL %r", label, message)
    return message


# ------------------------------------------------------------- validation
def valid_id(value):
    """IDs are positive integers (type(...) is int rejects True/False and "7")."""
    return type(value) is int and value > 0


def validate_url(url):
    """A callback must be an absolute http(s) URL, e.g. http://127.0.0.1:5003/events."""
    if not isinstance(url, str) or not url.strip():
        raise ValueError("url must be a nonempty string")
    parsed = urlparse(url.strip())
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError("url must be an absolute http:// or https:// URL")
    return url.strip()


def validate_event(event):
    """Check the envelope; for post.created also check what Feed will check.

    Catching a bad event here gives the publisher ONE clear 400 instead of the
    same rejection from every subscriber. Returns (event_id, topic).
    """
    if not isinstance(event, dict):
        raise ValueError("event must be a JSON object")
    event_id = event.get("event_id")
    if not isinstance(event_id, str) or not event_id.strip():
        raise ValueError("event_id must be a nonempty string")
    topic = event.get("type")
    if not isinstance(topic, str) or not topic.strip():
        raise ValueError("type must be a nonempty string")
    if topic == "post.created":
        post = event.get("post")
        if not isinstance(post, dict):
            raise ValueError("post.created needs a post object")
        if not valid_id(post.get("id")) or not valid_id(post.get("author")):
            raise ValueError("post id and author must be positive integers")
        for field in ("timestamp", "content", "caption"):
            if not isinstance(post.get(field), str):
                raise ValueError(f"post {field} must be a string")
        stamp = datetime.fromisoformat(post["timestamp"].replace("Z", "+00:00"))
        if stamp.tzinfo is None:
            raise ValueError("post timestamp must include its UTC offset")
    return event_id, topic


# ----------------------------------------------------------------- broker
class Broker:
    def __init__(self, logger, timeout=2.0, attempts=3, backoff=0.5):
        self.logger = logger
        self.timeout = timeout      # seconds per delivery attempt
        self.attempts = attempts    # tries per subscriber, first try included
        self.backoff = backoff      # first wait between tries; doubles each time
        self.subscriptions = {}     # topic -> ordered list of callback URLs
        self.lock = threading.Lock()  # protects self.subscriptions

    # --- subscription table -------------------------------------------------
    def subscribe(self, topic, url):
        """Return True if newly added, False if it was already registered."""
        with self.lock:
            urls = self.subscriptions.setdefault(topic, [])
            if url in urls:
                return False
            urls.append(url)
            return True

    def unsubscribe(self, topic, url):
        with self.lock:
            urls = self.subscriptions.get(topic, [])
            if url not in urls:
                return False
            urls.remove(url)
            return True

    def snapshot(self):
        with self.lock:
            return [{"topic": t, "url": u}
                    for t, urls in self.subscriptions.items() for u in urls]

    def subscribers_for(self, topic):
        with self.lock:                       # copy so delivery never holds the lock
            return list(self.subscriptions.get(topic, []))

    # --- delivery -----------------------------------------------------------
    def deliver(self, url, wire, event_id, request_id):
        """Send one event to ONE subscriber, retrying transient failures.

        Always returns a result dict; it never raises, so one subscriber
        cannot break delivery to the others.
        """
        headers = {"Content-Type": "application/json", "X-Request-ID": request_id}
        label = f"{request_id} to={url}"
        result = {"url": url, "delivered": False, "attempts": 0,
                  "status": None, "duplicate": None, "error": None}
        for attempt in range(1, self.attempts + 1):
            result["attempts"] = attempt
            retryable = True
            self.logger.info("DELIVER_ATTEMPT event_id=%s attempt=%d/%d url=%s",
                             event_id, attempt, self.attempts, url)
            try:
                response = requests.post(url, data=wire, headers=headers,
                                         timeout=self.timeout)
                result["status"] = response.status_code
                self.logger.info("DELIVER_REPLY event_id=%s status=%s url=%s",
                                 event_id, response.status_code, url)
                if response.ok:
                    ack = unmarshal(response.content, self.logger, label)
                    if isinstance(ack, dict) and ack.get("success") is True:
                        result.update(delivered=True, error=None)
                        if isinstance(ack.get("duplicate"), bool):
                            result["duplicate"] = ack["duplicate"]
                        break
                    result["error"] = "2xx reply without success=true"
                    retryable = False          # the subscriber answered; resending won't help
                else:
                    result["error"] = f"HTTP {response.status_code}"
                    # 5xx/408/429 may be temporary; other 4xx are the event's fault.
                    retryable = response.status_code >= 500 or response.status_code in (408, 429)
            except requests.Timeout:
                result["error"] = f"timeout after {self.timeout}s"
            except requests.RequestException as error:
                result["error"] = f"{type(error).__name__}"
            except ValueError:
                result["error"] = "2xx reply was not valid JSON"
                retryable = False
            except Exception as error:                  # last-resort isolation
                result["error"] = f"unexpected {type(error).__name__}: {error}"
                retryable = False
            self.logger.warning("DELIVER_FAILED event_id=%s attempt=%d url=%s error=%s",
                                event_id, attempt, url, result["error"])
            if not retryable or attempt == self.attempts:
                break
            time.sleep(self.backoff * 2 ** (attempt - 1))
        self.logger.info("DELIVER_RESULT event_id=%s delivered=%s attempts=%d url=%s",
                         event_id, result["delivered"], result["attempts"], url)
        return result

    def publish(self, event, request_id):
        """Deliver an event to every subscriber whose topic equals event['type']."""
        event_id, topic = validate_event(event)
        subscribers = self.subscribers_for(topic)
        self.logger.info("PUBLISH event_id=%s topic=%s matched=%d request_id=%s",
                         event_id, topic, len(subscribers), request_id)
        results = []
        if subscribers:
            wire = marshal(event, self.logger, f"{request_id} event_id={event_id}")
            # One worker per subscriber: deliveries run in parallel, so a slow
            # subscriber delays only its own result, not the others.
            with ThreadPoolExecutor(max_workers=len(subscribers)) as pool:
                results = list(pool.map(
                    lambda url: self.deliver(url, wire, event_id, request_id),
                    subscribers))
        delivered = sum(1 for r in results if r["delivered"])
        report = {"success": True, "event_id": event_id, "topic": topic,
                  "matched": len(results), "delivered": delivered,
                  "failed": len(results) - delivered,
                  "all_delivered": delivered == len(results), "results": results}
        self.logger.info("PUBLISH_DONE event_id=%s matched=%d delivered=%d failed=%d",
                         event_id, report["matched"], delivered, report["failed"])
        return report


# -------------------------------------------------------------- HTTP layer
def create_app(timeout=2.0, attempts=3, backoff=0.5, log_dir="logs"):
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = 1024 * 1024
    logger = make_logger("broker", log_dir)
    broker = Broker(logger, timeout, attempts, backoff)
    app.config["BROKER"] = broker

    @app.before_request
    def log_request():
        g.request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
        logger.info("RECV %s %s request_id=%s", request.method, request.path, g.request_id)

    @app.after_request
    def log_reply(response):
        response.headers["X-Request-ID"] = g.request_id
        logger.info("SEND status=%s request_id=%s body=%s",
                    response.status_code, g.request_id, response.get_data(as_text=True))
        return response

    def read_body():
        """Unmarshal the request body (logged) and require a JSON object."""
        body = unmarshal(request.get_data(), logger, g.request_id)
        if not isinstance(body, dict):
            raise ValueError("body must be a JSON object")
        return body

    def subscription_fields(body):
        topic = body.get("topic")
        if not isinstance(topic, str) or not topic.strip():
            raise ValueError("topic must be a nonempty string")
        return topic.strip(), validate_url(body.get("url"))

    @app.get("/health")
    def health():
        return jsonify({"service": "broker", "status": "ok"})

    @app.get("/subscriptions")
    def subscriptions():
        return jsonify({"success": True, "subscriptions": broker.snapshot()})

    @app.post("/subscribe")
    def subscribe():
        try:
            topic, url = subscription_fields(read_body())
        except ValueError as error:           # includes JSON and UTF-8 errors
            return jsonify({"success": False, "error": str(error)}), 400
        added = broker.subscribe(topic, url)
        logger.info("SUBSCRIBED topic=%s url=%s new=%s", topic, url, added)
        return jsonify({"success": True, "topic": topic, "url": url,
                        "duplicate": not added})

    @app.post("/unsubscribe")
    def unsubscribe():
        try:
            topic, url = subscription_fields(read_body())
        except ValueError as error:
            return jsonify({"success": False, "error": str(error)}), 400
        removed = broker.unsubscribe(topic, url)
        logger.info("UNSUBSCRIBED topic=%s url=%s removed=%s", topic, url, removed)
        return jsonify({"success": True, "removed": removed})

    @app.post("/publish")
    def publish():
        try:
            event = read_body()
            validate_event(event)
        except (ValueError, TypeError) as error:
            return jsonify({"success": False, "error": str(error)}), 400
        return jsonify(broker.publish(event, g.request_id))

    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Broker service for Milestone 2")
    parser.add_argument("--host", default="127.0.0.1",
                        help="use 0.0.0.0 only if other computers must reach the broker")
    parser.add_argument("--port", type=int, default=5005)
    parser.add_argument("--timeout", type=float, default=2.0,
                        help="seconds per delivery attempt (default 2)")
    parser.add_argument("--attempts", type=int, default=3,
                        help="delivery tries per subscriber, first try included (default 3)")
    parser.add_argument("--backoff", type=float, default=0.5,
                        help="seconds before the first retry; doubles each retry")
    args = parser.parse_args()
    app = create_app(args.timeout, max(1, args.attempts), args.backoff)
    app.config["BROKER"].logger.info(
        "BROKER_LISTEN %s:%s timeout=%ss attempts=%s", args.host, args.port,
        args.timeout, max(1, args.attempts))
    app.run(host=args.host, port=args.port, threaded=True, debug=False,
            use_reloader=False)
