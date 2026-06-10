import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from deskbridge.agent.runner import AgentRunner, _APPROVAL_INSTRUCTION
from deskbridge.config import ProjectConfig

PROJ = ProjectConfig(
    id="proj-1", name="MyProj", repo_path="/repo",
    identity="alice", escalation_dm_target="npub1op", adapter="claude-code",
)
PROJ_NO_DM = ProjectConfig(
    id="proj-1", name="MyProj", repo_path="/repo",
    identity="alice", escalation_dm_target="", adapter="claude-code",
)


def _row(**kwargs):
    m = MagicMock()
    m.__getitem__ = MagicMock(side_effect=lambda k: kwargs[k])
    m.get = MagicMock(side_effect=lambda k, d=None: kwargs.get(k, d))
    return m


def make_store():
    store = MagicMock()
    store.upsert_agent_run = AsyncMock()
    store.update_agent_run = AsyncMock()
    store.complete_work_item = AsyncMock()
    store.insert_outbox_item = AsyncMock()
    store.log_audit = AsyncMock()
    return store


def make_broker(session_id="sess-1"):
    broker = MagicMock()
    broker.get_session_id = AsyncMock(return_value=session_id)
    return broker


async def _stdout(*lines):
    for line in lines:
        yield line


def make_proc(*, returncode=0, stdout_lines=None):
    proc = MagicMock()
    proc.returncode = returncode
    proc.terminate = MagicMock()
    proc.kill = MagicMock()
    proc.stdout = _stdout(*(stdout_lines or [b"agent output\n"]))
    proc.wait = AsyncMock(return_value=returncode)
    return proc


def make_runner(work_item, project=PROJ, *, store=None, broker=None, client=None,
                timeout_secs=5.0, heartbeat_interval_secs=9999.0):
    return AgentRunner(
        work_item=work_item,
        project=project,
        run_id="run-1",
        store=store or make_store(),
        client=client or MagicMock(),
        broker=broker or make_broker(),
        timeout_secs=timeout_secs,
        heartbeat_interval_secs=heartbeat_interval_secs,
    )


async def test_runner_success_marks_done_and_writes_outbox():
    work_item = _row(id="wi-1", source_type="dm", source_id="msg-1",
                     summary="fix bug", payload_json="{}")
    store = make_store()
    proc = make_proc(returncode=0, stdout_lines=[b"line 1\n", b"line 2\n"])

    with patch("asyncio.create_subprocess_exec", return_value=proc):
        runner = make_runner(work_item, store=store)
        await runner.run()

    store.upsert_agent_run.assert_awaited_once_with(
        id="run-1", work_item_id="wi-1", adapter_type="claude-code"
    )
    store.update_agent_run.assert_awaited()
    # Last call to update_agent_run should include status='done'
    final_call_kwargs = store.update_agent_run.call_args_list[-1].kwargs
    assert final_call_kwargs["status"] == "done"
    store.complete_work_item.assert_awaited_once_with("wi-1", status="done")
    store.insert_outbox_item.assert_awaited_once()
    outbox_kwargs = store.insert_outbox_item.call_args.kwargs
    assert outbox_kwargs["dest_pubkey"] == "npub1op"
    assert outbox_kwargs["idempotency_key"] == "deskbridge:run-1:result_notify"


async def test_runner_nonzero_exit_marks_failed():
    work_item = _row(id="wi-1", source_type="dm", source_id="msg-1",
                     summary="fix bug", payload_json="{}")
    store = make_store()
    proc = make_proc(returncode=1, stdout_lines=[b"error output\n"])

    with patch("asyncio.create_subprocess_exec", return_value=proc):
        runner = make_runner(work_item, store=store)
        await runner.run()

    final_call_kwargs = store.update_agent_run.call_args_list[-1].kwargs
    assert final_call_kwargs["status"] == "failed"
    store.complete_work_item.assert_awaited_once_with("wi-1", status="failed")
    store.insert_outbox_item.assert_awaited_once()


async def test_runner_timeout_sends_sigterm_marks_interrupted():
    work_item = _row(id="wi-1", source_type="dm", source_id="msg-1",
                     summary="fix bug", payload_json="{}")
    store = make_store()
    proc = make_proc(stdout_lines=[])

    call_count = 0

    async def hanging_wait():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            await asyncio.sleep(9999)  # triggers TimeoutError on first call
        # second call (after SIGTERM) returns immediately

    proc.wait = hanging_wait

    with patch("asyncio.create_subprocess_exec", return_value=proc):
        runner = make_runner(work_item, store=store, timeout_secs=0.05)
        await runner.run()

    proc.terminate.assert_called_once()
    final_call_kwargs = store.update_agent_run.call_args_list[-1].kwargs
    assert final_call_kwargs["status"] == "interrupted"
    store.complete_work_item.assert_awaited_once_with("wi-1", status="interrupted")


async def test_runner_cancelled_sends_sigterm_and_reraises():
    work_item = _row(id="wi-1", source_type="dm", source_id="msg-1",
                     summary="fix bug", payload_json="{}")
    store = make_store()
    proc = make_proc(stdout_lines=[])

    call_count = 0

    async def hang_then_fast():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            await asyncio.sleep(9999)  # first call hangs (triggers CancelledError path)
        # second call returns immediately (after SIGTERM cleanup wait)

    proc.wait = hang_then_fast

    with patch("asyncio.create_subprocess_exec", return_value=proc):
        runner = make_runner(work_item, store=store, timeout_secs=9999.0)
        task = asyncio.create_task(runner.run())
        await asyncio.sleep(0.02)  # let subprocess "start"
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass  # expected

    proc.terminate.assert_called_once()
    proc.kill.assert_not_called()  # second proc.wait() returned fast, no need for SIGKILL
    # complete_work_item must NOT be called — CancelledError skips steps 6-9
    store.complete_work_item.assert_not_awaited()


async def test_runner_kanban_source_updates_board_card():
    work_item = _row(id="wi-1", source_type="kanban", source_id="card-42",
                     summary="kanban task", payload_json="{}")
    store = make_store()
    broker = make_broker(session_id="sess-1")
    client = MagicMock()
    client.call_tool = AsyncMock(return_value={"updated": True})
    proc = make_proc(returncode=0, stdout_lines=[b"done\n"])

    with patch("asyncio.create_subprocess_exec", return_value=proc):
        runner = make_runner(work_item, store=store, broker=broker, client=client)
        await runner.run()

    client.call_tool.assert_awaited_once()
    board_call_args = client.call_tool.call_args
    assert board_call_args[0][0] == "update_board_card"
    assert board_call_args[0][1]["card_id"] == "card-42"
    assert board_call_args[0][1]["idempotency_key"] == "deskbridge:run-1:board_update"


async def test_runner_no_session_skips_board_update():
    work_item = _row(id="wi-1", source_type="kanban", source_id="card-42",
                     summary="kanban task", payload_json="{}")
    store = make_store()
    broker = make_broker(session_id=None)
    client = MagicMock()
    client.call_tool = AsyncMock()
    proc = make_proc(returncode=0, stdout_lines=[b"done\n"])

    with patch("asyncio.create_subprocess_exec", return_value=proc):
        runner = make_runner(work_item, store=store, broker=broker, client=client)
        await runner.run()  # must not raise

    client.call_tool.assert_not_awaited()


async def test_runner_empty_escalation_target_skips_outbox():
    work_item = _row(id="wi-1", source_type="dm", source_id="msg-1",
                     summary="fix bug", payload_json="{}")
    store = make_store()
    proc = make_proc(returncode=0, stdout_lines=[b"done\n"])

    with patch("asyncio.create_subprocess_exec", return_value=proc):
        runner = make_runner(work_item, project=PROJ_NO_DM, store=store)
        await runner.run()

    store.insert_outbox_item.assert_not_awaited()


async def test_runner_unexpected_exception_marks_failed():
    work_item = _row(id="wi-1", source_type="dm", source_id="msg-1",
                     summary="fix bug", payload_json="{}")
    store = make_store()
    store.upsert_agent_run = AsyncMock(side_effect=RuntimeError("db exploded"))

    with patch("asyncio.create_subprocess_exec", side_effect=RuntimeError("no process")):
        runner = make_runner(work_item, store=store)
        await runner.run()  # must not raise

    store.complete_work_item.assert_awaited_once_with("wi-1", "failed")


async def test_runner_prompt_starts_with_approval_instruction():
    work_item = _row(id="wi-1", source_type="dm", source_id="msg-1",
                     summary="do the thing", payload_json='{"x": 1}')
    store = make_store()
    proc = make_proc(returncode=0, stdout_lines=[b"done\n"])

    captured_cmd = []

    def capture_exec(*args, **kwargs):
        captured_cmd.extend(args)
        return proc

    with patch("asyncio.create_subprocess_exec", side_effect=capture_exec):
        runner = make_runner(work_item, store=store)
        await runner.run()

    cmd = list(captured_cmd)
    msg_idx = cmd.index("--message")
    prompt = cmd[msg_idx + 1]

    assert prompt.startswith(_APPROVAL_INSTRUCTION)
    assert len(prompt) <= 4000


async def test_runner_prompt_instruction_not_truncated_on_long_task():
    long_payload = "x" * 4000
    work_item = _row(id="wi-1", source_type="dm", source_id="msg-1",
                     summary="long task", payload_json=long_payload)
    store = make_store()
    proc = make_proc(returncode=0, stdout_lines=[b"done\n"])

    captured_cmd = []

    def capture_exec(*args, **kwargs):
        captured_cmd.extend(args)
        return proc

    with patch("asyncio.create_subprocess_exec", side_effect=capture_exec):
        runner = make_runner(work_item, store=store)
        await runner.run()

    cmd = list(captured_cmd)
    msg_idx = cmd.index("--message")
    prompt = cmd[msg_idx + 1]

    instruction_len = len(_APPROVAL_INSTRUCTION)
    max_task_len = 4000 - instruction_len
    task_portion = prompt[instruction_len:]
    expected_task = f"long task\n\n{long_payload}"[:max_task_len]

    assert len(prompt) == 4000
    assert prompt[:instruction_len] == _APPROVAL_INSTRUCTION
    assert task_portion == expected_task


async def test_scheduled_completion_dms_operator_npub():
    work_item = _row(
        id="wi-1",
        source_type="scheduled",
        source_id="scheduled",
        summary="Check status.",
        payload_json='{"prompt": "Check status.", "operator_npub": "npub1op"}',
    )
    store = make_store()
    proc = make_proc(returncode=0, stdout_lines=[b"check-in complete\n"])

    # Use PROJ_NO_DM so step 8 (escalation_dm_target) does not fire,
    # keeping insert_outbox_item calls attributable solely to step 10.
    with patch("asyncio.create_subprocess_exec", return_value=proc):
        runner = make_runner(work_item, project=PROJ_NO_DM, store=store)
        await runner.run()

    outbox_calls = store.insert_outbox_item.call_args_list
    checkin_calls = [
        c for c in outbox_calls
        if "checkin_result" in (c.kwargs.get("idempotency_key") or "")
    ]
    assert len(checkin_calls) == 1
    assert checkin_calls[0].kwargs["dest_pubkey"] == "npub1op"
    assert "check-in complete" in checkin_calls[0].kwargs["message_text"]


async def test_non_scheduled_completion_does_not_send_checkin_dm():
    work_item = _row(
        id="wi-1",
        source_type="dm",
        source_id="msg-1",
        summary="fix bug",
        payload_json="{}",
    )
    store = make_store()
    proc = make_proc(returncode=0)

    with patch("asyncio.create_subprocess_exec", return_value=proc):
        runner = make_runner(work_item, project=PROJ_NO_DM, store=store)
        await runner.run()

    outbox_calls = store.insert_outbox_item.call_args_list
    checkin_calls = [
        c for c in outbox_calls
        if "checkin_result" in (c.kwargs.get("idempotency_key") or "")
    ]
    assert len(checkin_calls) == 0


async def test_scheduled_no_operator_npub_skips_checkin_dm():
    work_item = _row(
        id="wi-1",
        source_type="scheduled",
        source_id="scheduled",
        summary="Check status.",
        payload_json='{"prompt": "Check status."}',
    )
    store = make_store()
    proc = make_proc(returncode=0, stdout_lines=[b"done\n"])

    with patch("asyncio.create_subprocess_exec", return_value=proc):
        runner = make_runner(work_item, project=PROJ_NO_DM, store=store)
        await runner.run()

    outbox_calls = store.insert_outbox_item.call_args_list
    checkin_calls = [
        c for c in outbox_calls
        if "checkin_result" in (c.kwargs.get("idempotency_key") or "")
    ]
    assert len(checkin_calls) == 0


async def test_scheduled_malformed_payload_json_does_not_fail_run():
    work_item = _row(
        id="wi-1",
        source_type="scheduled",
        source_id="scheduled",
        summary="Check status.",
        payload_json="not-json",
    )
    store = make_store()
    proc = make_proc(returncode=0, stdout_lines=[b"done\n"])

    with patch("asyncio.create_subprocess_exec", return_value=proc):
        runner = make_runner(work_item, project=PROJ_NO_DM, store=store)
        await runner.run()  # must not raise

    store.complete_work_item.assert_awaited_once_with("wi-1", status="done")
    outbox_calls = store.insert_outbox_item.call_args_list
    checkin_calls = [
        c for c in outbox_calls
        if "checkin_result" in (c.kwargs.get("idempotency_key") or "")
    ]
    assert len(checkin_calls) == 0
