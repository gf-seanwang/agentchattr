import json
import multiprocessing
import sys
import tempfile
import time
import unittest
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from queue_utils import (
    append_queue_line,
    claim_queue_file,
    recover_processing_files,
)


# --- multiprocessing target functions ------------------------------------
# Must be module-level so they can be pickled under the spawn start method
# (default on macOS for Python 3.8+).


def _writer(queue_path: str, ids: list[str], delay: float) -> None:
    qf = Path(queue_path)
    for tid in ids:
        append_queue_line(qf, json.dumps({"trigger_id": tid}))
        if delay:
            time.sleep(delay)


def _claimer(queue_path: str, out_path: str, stop_after: float) -> None:
    qf = Path(queue_path)
    out = Path(out_path)
    end = time.monotonic() + stop_after
    while time.monotonic() < end:
        processing = claim_queue_file(qf)
        if processing is None:
            time.sleep(0.01)
            continue
        try:
            data = processing.read_text(encoding="utf-8")
            with out.open("a", encoding="utf-8") as f:
                f.write(data)
        finally:
            try:
                processing.unlink()
            except FileNotFoundError:
                pass


# --- tests ----------------------------------------------------------------


class ClaimQueueFileTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.queue_file = Path(self.tmpdir.name) / "test_queue.jsonl"

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_returns_none_when_missing(self):
        self.assertIsNone(claim_queue_file(self.queue_file))

    def test_returns_none_when_empty(self):
        self.queue_file.write_text("", encoding="utf-8")
        self.assertIsNone(claim_queue_file(self.queue_file))

    def test_renames_existing_queue(self):
        append_queue_line(self.queue_file, json.dumps({"trigger_id": "a"}))
        processing = claim_queue_file(self.queue_file)
        self.assertIsNotNone(processing)
        self.assertTrue(processing.exists())
        self.assertFalse(self.queue_file.exists())
        data = processing.read_text(encoding="utf-8")
        self.assertIn("a", data)


class RecoverProcessingFilesTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.queue_file = Path(self.tmpdir.name) / "test_queue.jsonl"

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_moves_processing_back_to_queue(self):
        leftover = self.queue_file.with_name(
            f"{self.queue_file.name}.processing.{uuid.uuid4().hex}"
        )
        leftover.write_text(json.dumps({"trigger_id": "xyz"}) + "\n", encoding="utf-8")

        recover_processing_files(self.queue_file)

        self.assertFalse(leftover.exists())
        self.assertTrue(self.queue_file.exists())
        self.assertIn("xyz", self.queue_file.read_text(encoding="utf-8"))

    def test_recovers_multiple_processing_files_in_order(self):
        ids = ["one", "two", "three"]
        for tid in ids:
            p = self.queue_file.with_name(
                f"{self.queue_file.name}.processing.{tid}"
            )
            p.write_text(json.dumps({"trigger_id": tid}) + "\n", encoding="utf-8")

        recover_processing_files(self.queue_file)

        text = self.queue_file.read_text(encoding="utf-8")
        for tid in ids:
            self.assertIn(tid, text)
        # All processing files removed
        for tid in ids:
            self.assertFalse(
                self.queue_file.with_name(
                    f"{self.queue_file.name}.processing.{tid}"
                ).exists()
            )

    def test_no_op_when_no_processing_files(self):
        # Should not raise, should not create the queue file from nothing.
        recover_processing_files(self.queue_file)
        self.assertFalse(self.queue_file.exists())


class ConcurrentAppendClaimTests(unittest.TestCase):
    """Process-level race test: the real production race is cross-process
    (web/app process appending, wrapper process claiming). POSIX flock
    semantics across threads in the same process are weaker than across
    processes, so threading would be a false-positive harness."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.queue_file = Path(self.tmpdir.name) / "test_queue.jsonl"
        self.out_file = Path(self.tmpdir.name) / "claimed.jsonl"
        self.out_file.touch()

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_all_appended_ids_eventually_claimed(self):
        ids = [uuid.uuid4().hex for _ in range(200)]
        # Use spawn for portability (default on macOS already).
        ctx = multiprocessing.get_context("spawn")

        claimer = ctx.Process(
            target=_claimer,
            args=(str(self.queue_file), str(self.out_file), 4.0),
        )
        writer = ctx.Process(
            target=_writer,
            args=(str(self.queue_file), ids, 0.001),
        )
        claimer.start()
        writer.start()
        writer.join(timeout=10)
        self.assertFalse(writer.is_alive(), "writer did not finish in time")
        claimer.join(timeout=10)
        self.assertFalse(claimer.is_alive(), "claimer did not finish in time")

        # Final drain in case the timing left anything in the queue.
        processing = claim_queue_file(self.queue_file)
        if processing is not None:
            try:
                with self.out_file.open("a", encoding="utf-8") as f:
                    f.write(processing.read_text(encoding="utf-8"))
            finally:
                processing.unlink()

        observed = set()
        for line in self.out_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            tid = obj.get("trigger_id")
            if tid:
                observed.add(tid)

        missing = set(ids) - observed
        self.assertEqual(missing, set(), f"missing {len(missing)} ids: {list(missing)[:5]}")


class ConsumerUnlinkSemanticsTests(unittest.TestCase):
    """The wrapper.py / wrapper_api.py consumer pattern: unlink the
    claimed `.processing.*` file only after handling succeeds. On any
    failure (exception or handler reporting False), the file must stay
    on disk so startup recovery can replay it."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.queue_file = Path(self.tmpdir.name) / "test_queue.jsonl"

    def tearDown(self):
        self.tmpdir.cleanup()

    def _consume(self, handler):
        """Mirror the wrapper.py consumer pattern."""
        processing = claim_queue_file(self.queue_file)
        if processing is None:
            return None, None
        handled = False
        try:
            lines = processing.read_text(encoding="utf-8").splitlines(keepends=True)
            handler(lines)
            handled = True
        except Exception:
            pass  # leave on disk for recovery
        if handled:
            try:
                processing.unlink()
            except FileNotFoundError:
                pass
        return processing, handled

    def _consume_with_bool_handler(self, handler):
        """Mirror the wrapper_api.py consumer pattern with bool signal."""
        processing = claim_queue_file(self.queue_file)
        if processing is None:
            return None, None
        handled = False
        try:
            lines = processing.read_text(encoding="utf-8").splitlines(keepends=True)
            handled = bool(handler(lines))
        except Exception:
            pass
        if handled:
            try:
                processing.unlink()
            except FileNotFoundError:
                pass
        return processing, handled

    def test_processing_file_kept_when_handler_raises(self):
        append_queue_line(self.queue_file, json.dumps({"trigger_id": "abc"}))

        def boom(lines):
            raise RuntimeError("simulated inject failure")

        processing, handled = self._consume(boom)

        self.assertIsNotNone(processing)
        self.assertFalse(handled)
        self.assertTrue(processing.exists(),
                        "processing file must remain on disk for recovery")

    def test_processing_file_unlinked_when_handler_succeeds(self):
        append_queue_line(self.queue_file, json.dumps({"trigger_id": "abc"}))

        seen = []

        def ok(lines):
            seen.extend(lines)

        processing, handled = self._consume(ok)

        self.assertIsNotNone(processing)
        self.assertTrue(handled)
        self.assertFalse(processing.exists(),
                         "processing file must be removed after successful handling")
        self.assertEqual(len(seen), 1)

    def test_malformed_only_file_is_unlinked(self):
        # Write a non-JSON line directly via the lock helper.
        append_queue_line(self.queue_file, "not-json-at-all")

        def no_trigger_handler(lines):
            # In real code, has_trigger would be False and we'd skip the
            # inject path. Returning normally is correct.
            return

        processing, handled = self._consume(no_trigger_handler)

        self.assertIsNotNone(processing)
        self.assertTrue(handled,
                        "malformed-only files should be classified as handled")
        self.assertFalse(processing.exists(),
                         "malformed-only files must not accumulate on disk")

    def test_processing_file_kept_when_bool_handler_returns_false(self):
        """wrapper_api.py: handle_trigger returns False on swallowed
        exception. The outer consumer must NOT unlink in that case."""
        append_queue_line(self.queue_file, json.dumps({"trigger_id": "abc"}))

        def bool_handler_fail(lines):
            # Simulate handle_trigger() catching an HTTP error and reporting False.
            return False

        processing, handled = self._consume_with_bool_handler(bool_handler_fail)

        self.assertIsNotNone(processing)
        self.assertFalse(handled)
        self.assertTrue(processing.exists(),
                        "processing file must remain when bool handler reports False")

    def test_processing_file_unlinked_when_bool_handler_returns_true(self):
        append_queue_line(self.queue_file, json.dumps({"trigger_id": "abc"}))

        def bool_handler_ok(lines):
            return True

        processing, handled = self._consume_with_bool_handler(bool_handler_ok)

        self.assertIsNotNone(processing)
        self.assertTrue(handled)
        self.assertFalse(processing.exists())


class PerChannelRequeueTests(unittest.TestCase):
    """The wrapper_api.py consumer: per-channel handling. Failed channels
    have their lines requeued via append_queue_line; the `.processing.*`
    file is always unlinked once read (only consumer-level read/parse
    exceptions leave the file intact)."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.queue_file = Path(self.tmpdir.name) / "test_queue.jsonl"

    def tearDown(self):
        self.tmpdir.cleanup()

    def _consume_per_channel(self, channel_handler):
        """Mirror the wrapper_api.py per-channel-requeue consumer."""
        processing = claim_queue_file(self.queue_file)
        if processing is None:
            return None, None
        consumed = False
        requeued_count = 0
        try:
            lines = processing.read_text(encoding="utf-8").splitlines(keepends=True)
            channel_lines: dict[str, list[str]] = {}
            for raw in lines:
                stripped = raw.strip()
                if not stripped:
                    continue
                try:
                    data = json.loads(stripped)
                    ch = data.get("channel", "general") if isinstance(data, dict) else "general"
                except json.JSONDecodeError:
                    ch = "general"
                channel_lines.setdefault(ch, []).append(
                    raw if raw.endswith("\n") else raw + "\n"
                )

            failed_lines: list[str] = []
            for ch, ch_lines in channel_lines.items():
                if not channel_handler(ch):
                    failed_lines.extend(ch_lines)

            if failed_lines:
                for fl in failed_lines:
                    append_queue_line(self.queue_file, fl.rstrip("\n"))
                requeued_count = len(failed_lines)
            consumed = True
        except Exception:
            pass

        if consumed:
            try:
                processing.unlink()
            except FileNotFoundError:
                pass
        return processing, requeued_count

    def test_full_success_drops_all_lines(self):
        append_queue_line(self.queue_file, json.dumps({"trigger_id": "a", "channel": "ch1"}))
        append_queue_line(self.queue_file, json.dumps({"trigger_id": "b", "channel": "ch2"}))

        processing, requeued = self._consume_per_channel(lambda ch: True)

        self.assertIsNotNone(processing)
        self.assertEqual(requeued, 0)
        self.assertFalse(processing.exists())
        # Queue file is gone (claimed and unlinked, nothing requeued).
        self.assertFalse(self.queue_file.exists())

    def test_partial_failure_requeues_only_failed_channels(self):
        append_queue_line(self.queue_file, json.dumps({"trigger_id": "a", "channel": "ok"}))
        append_queue_line(self.queue_file, json.dumps({"trigger_id": "b", "channel": "bad"}))

        processing, requeued = self._consume_per_channel(lambda ch: ch == "ok")

        self.assertIsNotNone(processing)
        self.assertEqual(requeued, 1)
        self.assertFalse(processing.exists())
        # Queue file now contains only the failed channel's line.
        text = self.queue_file.read_text(encoding="utf-8")
        self.assertIn("\"b\"", text)
        self.assertNotIn("\"a\"", text)

    def test_full_failure_requeues_all_lines_and_unlinks_processing(self):
        append_queue_line(self.queue_file, json.dumps({"trigger_id": "a", "channel": "ch1"}))
        append_queue_line(self.queue_file, json.dumps({"trigger_id": "b", "channel": "ch2"}))

        processing, requeued = self._consume_per_channel(lambda ch: False)

        self.assertIsNotNone(processing)
        self.assertEqual(requeued, 2)
        self.assertFalse(
            processing.exists(),
            "processing file must be unlinked once failed lines are requeued",
        )
        text = self.queue_file.read_text(encoding="utf-8")
        self.assertIn("\"a\"", text)
        self.assertIn("\"b\"", text)


if __name__ == "__main__":
    unittest.main()
