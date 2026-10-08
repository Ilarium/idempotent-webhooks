import unittest

from idempotent_webhooks import IdempotentProcessor, idempotency_key


class IdempotencyTests(unittest.TestCase):
    def setUp(self):
        self.leads = []
        self.processor = IdempotentProcessor(max_attempts=3)

    def create_lead(self, payload):
        self.leads.append(payload["email"])

    def test_key_depends_on_identity_not_time(self):
        a = idempotency_key("crm", "42", "lead.created")
        b = idempotency_key("crm", "42", "lead.created")
        self.assertEqual(a, b)
        self.assertNotEqual(a, idempotency_key("crm", "43", "lead.created"))
        self.assertNotEqual(a, idempotency_key("crm", "42", "lead.updated"))

    def test_retry_of_same_event_creates_one_lead(self):
        event = ("crm", "42", "lead.created", {"email": "a@example.com"})
        self.assertEqual(self.processor.process(*event, self.create_lead), "done")
        self.assertEqual(self.processor.process(*event, self.create_lead), "duplicate")
        self.assertEqual(self.leads, ["a@example.com"])

    def test_failed_event_is_retried_then_dead_lettered(self):
        def always_fails(payload):
            raise RuntimeError("crm timeout")

        result = self.processor.process("crm", "7", "lead.created", {"email": "b@example.com"}, always_fails)
        self.assertEqual(result, "failed")
        self.assertEqual(self.processor.retry_failed(always_fails), {"done": 0, "failed": 1, "dead": 0})
        self.assertEqual(self.processor.retry_failed(always_fails), {"done": 0, "failed": 0, "dead": 1})
        dead = self.processor.dead_letters()
        self.assertEqual(len(dead), 1)
        self.assertEqual(dead[0]["external_id"], "7")
        self.assertEqual(dead[0]["attempts"], 3)

    def test_failed_event_succeeds_on_retry(self):
        state = {"fail": True}

        def flaky(payload):
            if state["fail"]:
                raise RuntimeError("temporary")
            self.create_lead(payload)

        self.assertEqual(self.processor.process("crm", "9", "lead.created", {"email": "c@example.com"}, flaky), "failed")
        state["fail"] = False
        self.assertEqual(self.processor.retry_failed(flaky), {"done": 1, "failed": 0, "dead": 0})
        self.assertEqual(self.leads, ["c@example.com"])

    def test_reconcile_finds_records_that_never_arrived(self):
        self.processor.process("crm", "1", "lead.created", {"email": "1@example.com"}, self.create_lead)
        self.processor.process("crm", "2", "lead.created", {"email": "2@example.com"}, self.create_lead)
        missing = self.processor.reconcile("crm", "lead.created", ["1", "2", "3", "4"])
        self.assertEqual(missing, {"3", "4"})


if __name__ == "__main__":
    unittest.main()
