import json
import uuid
import structlog

from deskbridge.db.store import Store
from deskbridge.mcp.client import McpClient, McpToolError

log = structlog.get_logger()

_FAILURE_REPLY = "Failed to record decision — please check logs."
_STALE_REPLY = "Decision received, but the approval was already resolved or has expired."


async def _resolve_and_audit(
    store: Store, account_id: str, row: dict, local_status: str
) -> None:
    await store.resolve_approval(row["id"], local_status)
    try:
        await store.log_audit(
            id=str(uuid.uuid4()),
            event_type="approval_resolved",
            identity_id=account_id,
            work_item_id=row["work_item_id"],
            payload_json=json.dumps({"approval_id": row["id"], "resolution": local_status}),
        )
    except Exception:
        log.warning("audit_log_failed", event_type="approval_resolved")


async def resolve_approval_via_mcp(
    *,
    store: Store,
    client: McpClient,
    identity_label: str,
    account_id: str,
    row: dict,
    mcp_approval_id: str,
    session_id: str,
    approved: bool,
) -> tuple[bool, str]:
    """Forward an operator decision to MCP and sync the local approval row.

    Returns (resolved, reply): resolved is True when the local approval row
    reached a terminal status; reply is the operator-facing message.
    """
    try:
        result = await client.call_tool(
            "respond_to_approval",
            {
                "session_id": session_id,
                "approval_request_id": mcp_approval_id,
                "approved": approved,
                "note": None,
            },
        )
        if not (isinstance(result, dict) and result.get("ok") is True):
            log.error(
                "approval_resolution_unexpected_response",
                identity=identity_label,
                mcp_approval_id=mcp_approval_id,
            )
            return False, _FAILURE_REPLY
        returned_id = result.get("approval_request_id")
        if returned_id != mcp_approval_id:
            log.error(
                "approval_resolution_id_mismatch",
                identity=identity_label,
                sent=mcp_approval_id,
                returned=returned_id,
            )
            return False, _FAILURE_REPLY
        response_status = result.get("status", "")
        if response_status == "approved":
            local_status = "approved"
        elif response_status == "denied":
            local_status = "rejected"
        else:
            log.error(
                "approval_resolution_unknown_response_status",
                identity=identity_label,
                status=response_status,
                mcp_approval_id=mcp_approval_id,
            )
            return False, _FAILURE_REPLY
        expected_response_status = "approved" if approved else "denied"
        if response_status != expected_response_status:
            log.error(
                "approval_resolution_status_mismatch",
                identity=identity_label,
                sent_approved=approved,
                response_status=response_status,
                mcp_approval_id=mcp_approval_id,
            )
            return False, _FAILURE_REPLY
        await _resolve_and_audit(store, account_id, row, local_status)
        log.info(
            "approval_resolution_resolved",
            identity=identity_label,
            mcp_approval_id=mcp_approval_id,
        )
        return True, ("Approved." if approved else "Denied.")
    except McpToolError as e:
        cat = e.category
        if cat == "approval_already_resolved":
            data_status = (e.data or {}).get("status", "")
            if data_status == "approved":
                local_status = "approved"
            elif data_status == "denied":
                local_status = "rejected"
            else:
                log.error(
                    "approval_resolution_already_resolved_unknown_status",
                    identity=identity_label,
                    data_status=data_status,
                    mcp_approval_id=mcp_approval_id,
                )
                return False, _FAILURE_REPLY
            await _resolve_and_audit(store, account_id, row, local_status)
            log.warning(
                "approval_resolution_already_resolved",
                identity=identity_label,
                mcp_approval_id=mcp_approval_id,
            )
            return True, _STALE_REPLY
        elif cat == "approval_expired":
            await _resolve_and_audit(store, account_id, row, "rejected")
            log.warning(
                "approval_resolution_expired",
                identity=identity_label,
                mcp_approval_id=mcp_approval_id,
            )
            return True, _STALE_REPLY
        elif cat == "approval_not_found":
            await _resolve_and_audit(store, account_id, row, "rejected")
            log.error(
                "approval_resolution_not_found",
                identity=identity_label,
                mcp_approval_id=mcp_approval_id,
            )
            return True, _STALE_REPLY
        else:
            log.error(
                "approval_resolution_error",
                identity=identity_label,
                message=e.mcp_error.message,
            )
            return False, _FAILURE_REPLY
    except Exception:
        log.exception("approval_resolution_error", identity=identity_label)
        return False, _FAILURE_REPLY
