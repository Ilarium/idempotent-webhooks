# idempotent-webhooks

A small, tested pattern for processing webhooks safely: no duplicates after retries, failures that are retried and then surfaced, and a nightly check that finds records that vanished silently.

## The problem

Webhooks are delivered **at least once**. If your handler creates a lead on every call, a timeout plus a retry gives you two leads (or two invoices, or two messages to a customer). The usual `if not exists` check in code is a race condition.

## The pattern

1. **Derive an idempotency key from the event identity** (source + external id + event type), never from the arrival time.
2. **Store the key before doing the work.** A `PRIMARY KEY` constraint in the database rejects the repeat atomically.
3. **Record failures.** A failed event keeps its error and attempt count and is retried.
4. **Dead-letter after N attempts.** The event moves to `dead` so that someone can be alerted instead of the lead silently vanishing.
5. **Reconcile.** Compare the ids the source says exist with the ids you processed to `done`. Step 5 is the one people skip, and it is the one that finds the records lost without an error.

## Usage

```python
from idempotent_webhooks import IdempotentProcessor

processor = IdempotentProcessor("events.db", max_attempts=3)

def create_lead(payload):
    ...  # call your CRM here

processor.process("crm", "42", "lead.created", {"email": "a@example.com"}, create_lead)  # 'done'
processor.process("crm", "42", "lead.created", {"email": "a@example.com"}, create_lead)  # 'duplicate'

processor.retry_failed(create_lead)   # run on a schedule
processor.dead_letters()              # alert on this list
processor.reconcile("crm", "lead.created", ids_from_crm_api)  # run nightly
```

```
python -m unittest -v
```

## Notes

- Standard library only (`sqlite3`). The same idea works with PostgreSQL (`INSERT ... ON CONFLICT DO NOTHING`) or Redis (`SET NX`).
- In production, acknowledge the webhook first and process it from a queue, so slow handlers do not trigger the sender's retries.
- A handler must still be safe to run twice if a crash happens between "work done" and "marked done". Prefer idempotent operations downstream (upserts, external idempotency keys).

## License

MIT
