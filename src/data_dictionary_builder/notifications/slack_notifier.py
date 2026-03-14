"""
Slack notifier for sending schema comparison reports.

Supports posting to public/private channels and sending direct messages
to individual users.  Uses the official ``slack-sdk`` library.

Install:
    pip install "data-dictionary-builder[slack]"
    uv add "data-dictionary-builder[slack]"

Quick start
-----------
::

    from data_dictionary_builder import SlackNotifier

    notifier = SlackNotifier(token="xoxb-your-bot-token")

    # Post to a channel
    notifier.send_comparison_report("#data-alerts", report, pdf_path="report.pdf")

    # Direct message a user
    notifier.send_comparison_report("@jane.doe", report)
    notifier.send_comparison_report("U0123456789", report)   # user ID also works

Slack app requirements
----------------------
Your bot needs these OAuth scopes:

    chat:write            — post messages to channels
    im:write              — open / write to DMs
    users:read            — look up user IDs by name (optional — only needed
                           if you pass display names like "@jane.doe")
    files:write           — upload files (PDF attachment)
    channels:read         — look up channel IDs by name (optional)

Create a Slack app at https://api.slack.com/apps, add the scopes above,
install it to your workspace, and copy the "Bot User OAuth Token" (xoxb-...).

Target resolution
-----------------
``send_*`` methods accept any of the following as the ``target`` argument:

    "#channel-name"   — public or private channel by name
    "C0123456789"     — channel ID directly
    "@username"       — DM a user by their display name or email prefix
    "U0123456789"     — user ID directly (DM)
    "W0123456789"     — enterprise grid user ID (DM)

When a display name is passed (``@name``), the notifier searches the workspace
member list to find the matching user ID before opening the DM.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger(__name__)


class SlackNotifier:
    """
    Send schema comparison reports and general messages via Slack.

    Supports both channel posts and direct messages.  File uploads (PDF
    reports) are sent using Slack's ``files.uploadV2`` API.

    Parameters
    ----------
    token     : Slack Bot User OAuth Token (``xoxb-...``).
                Falls back to the ``SLACK_BOT_TOKEN`` environment variable
                when not supplied explicitly.
    timeout   : HTTP request timeout in seconds (default 30).
    """

    def __init__(
        self,
        token: Optional[str] = None,
        timeout: int = 30,
    ) -> None:
        self._token   = token or os.getenv("SLACK_BOT_TOKEN", "")
        self._timeout = timeout
        self._client  = None   # lazy — created on first use

        # Detect wrong token type immediately rather than getting a cryptic
        # 'not_allowed_token_type' error from the API later.
        if self._token and self._token.startswith("xapp-"):
            raise ValueError(
                "The Slack token provided is an app-level token (xapp-…). "
                "Most Slack API methods require a Bot User OAuth Token (xoxb-…).\n"
                "To get the correct token:\n"
                "  1. Go to https://api.slack.com/apps → select your app\n"
                "  2. Navigate to 'OAuth & Permissions'\n"
                "  3. Copy the 'Bot User OAuth Token' (starts with xoxb-)\n"
                "  4. Update SLACK_BOT_TOKEN in your .env file."
            )

    # ── Client lifecycle ──────────────────────────────────────────────────────

    @property
    def client(self):
        """Return a cached ``slack_sdk.WebClient`` instance."""
        if self._client is None:
            try:
                from slack_sdk import WebClient
            except ImportError:
                raise ImportError(
                    "slack-sdk is not installed.\n"
                    "Install it with:  pip install \"data-dictionary-builder[slack]\"\n"
                    "                  uv add \"data-dictionary-builder[slack]\""
                )
            if not self._token:
                raise ValueError(
                    "No Slack token provided.  Pass token= to SlackNotifier() "
                    "or set the SLACK_BOT_TOKEN environment variable."
                )
            from slack_sdk import WebClient
            self._client = WebClient(token=self._token, timeout=self._timeout)
        return self._client

    # ── Target resolution ─────────────────────────────────────────────────────

    def _resolve_target(self, target: str) -> str:
        """
        Resolve *target* to a Slack channel ID suitable for ``chat.postMessage``.

        Rules:
        - Channel IDs (``C...``, ``G...``) and DM channel IDs (``D...``) are
          returned as-is.
        - ``#channel-name`` — looked up via ``conversations.list``.
        - ``@display-name`` or ``U.../W...`` user IDs — a DM channel is opened
          via ``conversations.open`` and its ``channel.id`` is returned.

        Parameters
        ----------
        target : Channel or user identifier (see module docstring for formats).

        Returns
        -------
        str — Slack channel ID.
        """
        target = target.strip()

        # Already a raw Slack ID
        if target.startswith(("C", "G", "D")):
            return target

        # User ID → open / return DM channel
        if target.startswith(("U", "W")):
            return self._open_dm(target)

        # "#channel-name" → look up channel ID
        if target.startswith("#"):
            return self._find_channel(target[1:])

        # "@display-name" or "@email-prefix" → find user ID then open DM
        if target.startswith("@"):
            user_id = self._find_user(target[1:])
            return self._open_dm(user_id)

        # Fallback: treat as a channel name without the hash
        return self._find_channel(target)

    def _open_dm(self, user_id: str) -> str:
        """Open a direct-message channel with *user_id* and return its channel ID."""
        resp = self.client.conversations_open(users=[user_id])
        if not resp["ok"]:
            raise RuntimeError(
                f"Could not open DM with user '{user_id}': {resp.get('error')}"
            )
        channel_id = resp["channel"]["id"]
        logger.debug("Opened DM channel %s for user %s", channel_id, user_id)
        return channel_id

    def _find_channel(self, name: str) -> str:
        """
        Find the channel ID for *name* by iterating ``conversations.list``.

        Searches public and private channels the bot has access to.
        """
        cursor = None
        while True:
            resp = self.client.conversations_list(
                types="public_channel,private_channel",
                limit=200,
                cursor=cursor,
            )
            if not resp["ok"]:
                raise RuntimeError(
                    f"Could not list channels: {resp.get('error')}"
                )
            for ch in resp["channels"]:
                if ch["name"] == name:
                    return ch["id"]
            cursor = resp.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                break
        raise ValueError(
            f"Slack channel '#{name}' not found.  "
            "Make sure the bot is invited to the channel."
        )

    def _find_user(self, name: str) -> str:
        """
        Find the user ID for *name* (display name or email prefix).

        Iterates ``users.list`` and matches against ``display_name``,
        ``real_name``, and the local part of ``profile.email``.
        """
        cursor = None
        name_lower = name.lower()
        while True:
            resp = self.client.users_list(limit=200, cursor=cursor)
            if not resp["ok"]:
                raise RuntimeError(
                    f"Could not list users: {resp.get('error')}"
                )
            for member in resp["members"]:
                if member.get("deleted") or member.get("is_bot"):
                    continue
                profile = member.get("profile", {})
                candidates = [
                    (profile.get("display_name") or "").lower(),
                    (profile.get("real_name") or "").lower(),
                    (profile.get("email") or "").split("@")[0].lower(),
                ]
                if name_lower in candidates:
                    return member["id"]
            cursor = resp.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                break
        raise ValueError(
            f"Slack user '@{name}' not found in workspace.  "
            "Check the display name, real name, or use the user ID directly."
        )

    # ── Core send methods ─────────────────────────────────────────────────────

    def send_message(
        self,
        target: str,
        text: str,
        blocks: Optional[List[Dict[str, Any]]] = None,
        thread_ts: Optional[str] = None,
        unfurl_links: bool = False,
    ) -> bool:
        """
        Send a plain text message (or Block Kit message) to a channel or DM.

        Parameters
        ----------
        target      : Channel (``#name``, ``C...``) or user (``@name``, ``U...``).
        text        : Fallback text shown in notifications and accessibility.
        blocks      : Optional list of Block Kit block dicts for rich formatting.
        thread_ts   : Reply in a thread by supplying the parent message timestamp.
        unfurl_links: Whether Slack should expand URLs in the message.

        Returns
        -------
        bool — ``True`` if the message was posted successfully.
        """
        try:
            channel_id = self._resolve_target(target)
            kwargs: Dict[str, Any] = {
                "channel":      channel_id,
                "text":         text,
                "unfurl_links": unfurl_links,
            }
            if blocks:
                kwargs["blocks"] = blocks
            if thread_ts:
                kwargs["thread_ts"] = thread_ts

            resp = self.client.chat_postMessage(**kwargs)
            if resp["ok"]:
                logger.info("Slack message sent to %s (channel=%s)", target, channel_id)
                return True
            logger.error("Slack message failed: %s", resp.get("error"))
            return False
        except Exception as exc:
            logger.error("Failed to send Slack message to '%s': %s", target, exc)
            return False

    def send_file(
        self,
        target: str,
        file_path: Union[str, Path],
        title: Optional[str] = None,
        comment: Optional[str] = None,
        thread_ts: Optional[str] = None,
    ) -> bool:
        """
        Upload a file (e.g. a PDF report) to a channel or DM.

        Uses Slack's ``files.uploadV2`` API which supports files of any size.

        Parameters
        ----------
        target      : Channel or user identifier.
        file_path   : Local path of the file to upload.
        title       : Display title shown above the file in Slack.
        comment     : Optional message text posted alongside the file.
        thread_ts   : Upload into a thread by supplying the parent timestamp.

        Returns
        -------
        bool — ``True`` if the file was uploaded successfully.
        """
        path = Path(file_path)
        if not path.exists():
            logger.error("File not found, cannot upload to Slack: %s", path)
            return False

        try:
            channel_id = self._resolve_target(target)
            kwargs: Dict[str, Any] = {
                "channel":   channel_id,
                "file":      str(path),
                "filename":  path.name,
                "title":     title or path.name,
            }
            if comment:
                kwargs["initial_comment"] = comment
            if thread_ts:
                kwargs["thread_ts"] = thread_ts

            resp = self.client.files_upload_v2(**kwargs)
            if resp["ok"]:
                logger.info(
                    "Slack file '%s' uploaded to %s (channel=%s)",
                    path.name, target, channel_id,
                )
                return True
            logger.error("Slack file upload failed: %s", resp.get("error"))
            return False
        except Exception as exc:
            logger.error("Failed to upload file '%s' to Slack '%s': %s", path, target, exc)
            return False

    # ── Report-specific methods ───────────────────────────────────────────────

    def send_comparison_report(
        self,
        target: str,
        report: Dict[str, Any],
        pdf_path: Optional[Union[str, Path]] = None,
        title: Optional[str] = None,
        pipeline_label: Optional[str] = None,
        thread_ts: Optional[str] = None,
    ) -> bool:
        """
        Send a formatted schema comparison report to a channel or DM.

        Posts a Block Kit message with a summary and key findings, then
        optionally uploads the PDF report as a file attachment.

        Parameters
        ----------
        target         : Channel (``#name``, ``C...``) or user (``@name``, ``U...``).
        report         : Comparison report dict from ``SchemaComparator`` /
                         ``run_compare_schemas``.
        pdf_path       : Optional path to the compiled PDF report to attach.
        title          : Custom message title / header text.
        pipeline_label : Optional pipeline name to include in the header.
        thread_ts      : Post into a thread by supplying the parent timestamp.

        Returns
        -------
        bool — ``True`` if the message (and file if provided) were sent.
        """
        blocks = self._build_report_blocks(report, title=title, pipeline_label=pipeline_label)

        # Fallback text for notifications (shown when blocks can't render)
        summary = report.get("summary", {})
        fallback = (
            f"Schema Comparison Report — "
            f"Missing tables: {summary.get('missing_tables_count', 0)}, "
            f"Missing columns: {summary.get('missing_columns_count', 0)}, "
            f"Type mismatches: {summary.get('type_mismatches_count', 0)}"
        )

        message_ok = self.send_message(
            target=target,
            text=fallback,
            blocks=blocks,
            thread_ts=thread_ts,
        )

        file_ok = True
        if pdf_path:
            pdf_path = Path(pdf_path)
            file_ok = self.send_file(
                target=target,
                file_path=pdf_path,
                title=title or "Schema Comparison Report.pdf",
                comment="Full report attached above.",
                thread_ts=thread_ts,
            )

        return message_ok and file_ok

    def send_pipeline_summary(
        self,
        target: str,
        pipeline_label: str,
        schemas_compared: List[str],
        summary: Dict[str, Any],
        pdf_path: Optional[Union[str, Path]] = None,
    ) -> bool:
        """
        Send a concise per-pipeline summary (useful in Airflow on_success_callback).

        Parameters
        ----------
        target           : Channel or user identifier.
        pipeline_label   : Pipeline name (e.g. ``"prod_to_analytics"``).
        schemas_compared : List of schema names that were compared.
        summary          : The ``"summary"`` sub-dict from the comparison report.
        pdf_path         : Optional path to the PDF report to attach.
        """
        has_issues = any(
            summary.get(k, 0) > 0
            for k in ("missing_tables_count", "missing_columns_count", "type_mismatches_count")
        )
        icon  = ":warning:" if has_issues else ":white_check_mark:"
        color = "#e01e5a"   if has_issues else "#2eb886"   # red / green

        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"{icon}  Pipeline: {pipeline_label}"},
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Schemas compared*\n{', '.join(f'`{s}`' for s in schemas_compared)}"},
                    {"type": "mrkdwn", "text": f"*Status*\n{'Issues found' if has_issues else 'All clear'}"},
                ],
            },
            {"type": "divider"},
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Missing tables*\n{summary.get('missing_tables_count', 0)}"},
                    {"type": "mrkdwn", "text": f"*Missing columns*\n{summary.get('missing_columns_count', 0)}"},
                    {"type": "mrkdwn", "text": f"*Type mismatches*\n{summary.get('type_mismatches_count', 0)}"},
                    {"type": "mrkdwn", "text": f"*Undocumented tables*\n{summary.get('tables_without_descriptions_count', 0)}"},
                ],
            },
        ]

        fallback = (
            f"{pipeline_label} — schemas: {schemas_compared} | "
            f"missing tables: {summary.get('missing_tables_count', 0)}, "
            f"missing columns: {summary.get('missing_columns_count', 0)}"
        )

        message_ok = self.send_message(target=target, text=fallback, blocks=blocks)

        file_ok = True
        if pdf_path:
            file_ok = self.send_file(
                target=target,
                file_path=pdf_path,
                title=f"{pipeline_label} — comparison report.pdf",
            )

        return message_ok and file_ok

    # ── Block Kit builder ─────────────────────────────────────────────────────

    def _build_report_blocks(
        self,
        report: Dict[str, Any],
        title: Optional[str] = None,
        pipeline_label: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Build a Block Kit block list for a comparison report.

        Slack limits messages to 50 blocks; detail lists are truncated to fit.
        """
        summary    = report.get("summary", {})
        comparison = report.get("comparison", {})
        yaml_gaps  = report.get("yaml_gaps", {})
        schemas    = report.get("schemas_compared", [])

        missing_tables  = comparison.get("missing_tables",  [])
        missing_columns = comparison.get("missing_columns", [])
        type_mismatches = comparison.get("type_mismatches", [])
        undoc_tables    = yaml_gaps.get("tables_without_descriptions",  [])

        has_issues = any([missing_tables, missing_columns, type_mismatches])
        status_icon = ":warning:" if has_issues else ":white_check_mark:"

        header_text = title or "Database Schema Comparison Report"
        if pipeline_label:
            header_text = f"{header_text}  —  {pipeline_label}"

        blocks: List[Dict[str, Any]] = [
            # ── Header ────────────────────────────────────────────────────────
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"{status_icon}  {header_text}"},
            },
        ]

        # ── Context: schemas compared ─────────────────────────────────────────
        if schemas:
            schema_text = "  ".join(f"`{s}`" for s in schemas)
            blocks.append({
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": f"*Schemas compared:*  {schema_text}"}],
            })

        blocks.append({"type": "divider"})

        # ── Summary stats ─────────────────────────────────────────────────────
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*Summary*"},
            "fields": [
                {"type": "mrkdwn", "text": f"*Missing tables*\n{summary.get('missing_tables_count', 0)}"},
                {"type": "mrkdwn", "text": f"*Missing columns*\n{summary.get('missing_columns_count', 0)}"},
                {"type": "mrkdwn", "text": f"*Type mismatches*\n{summary.get('type_mismatches_count', 0)}"},
                {"type": "mrkdwn", "text": f"*Undocumented tables*\n{summary.get('tables_without_descriptions_count', 0)}"},
                {"type": "mrkdwn", "text": f"*Undocumented columns*\n{summary.get('columns_without_descriptions_count', 0)}"},
            ],
        })

        # ── Missing tables detail (up to 10) ──────────────────────────────────
        if missing_tables:
            blocks.append({"type": "divider"})
            rows = "\n".join(
                f"• `{t['schema']}.{t['table']}` ({t.get('column_count', '?')} cols)"
                for t in missing_tables[:10]
            )
            if len(missing_tables) > 10:
                rows += f"\n_…and {len(missing_tables) - 10} more — see PDF for full list_"
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f":x:  *Missing tables in destination* ({len(missing_tables)})\n{rows}"},
            })

        # ── Missing columns detail (up to 15) ─────────────────────────────────
        if missing_columns:
            blocks.append({"type": "divider"})
            rows = "\n".join(
                f"• `{c['schema']}.{c['table']}.{c['column']}` — _{c.get('data_type', '?')}_"
                for c in missing_columns[:15]
            )
            if len(missing_columns) > 15:
                rows += f"\n_…and {len(missing_columns) - 15} more — see PDF for full list_"
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f":large_orange_circle:  *Missing columns in destination* ({len(missing_columns)})\n{rows}"},
            })

        # ── Type mismatches detail (up to 10) ─────────────────────────────────
        if type_mismatches:
            blocks.append({"type": "divider"})
            rows = "\n".join(
                f"• `{m['schema']}.{m['table']}.{m['column']}` — "
                f"source: `{m.get('source_type', '?')}` → dest: `{m.get('destination_type', '?')}`"
                for m in type_mismatches[:10]
            )
            if len(type_mismatches) > 10:
                rows += f"\n_…and {len(type_mismatches) - 10} more — see PDF for full list_"
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f":arrows_counterclockwise:  *Data type mismatches* ({len(type_mismatches)})\n{rows}"},
            })

        # ── Documentation gaps ────────────────────────────────────────────────
        if undoc_tables:
            blocks.append({"type": "divider"})
            rows = "  ".join(f"`{t}`" for t in undoc_tables[:12])
            if len(undoc_tables) > 12:
                rows += f"  _+{len(undoc_tables) - 12} more_"
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f":memo:  *Tables missing descriptions* ({len(undoc_tables)})\n{rows}"},
            })

        # ── Footer ────────────────────────────────────────────────────────────
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": (
                    ":bar_chart: Generated by *data-dictionary-builder*  |  "
                    "Full details in the attached PDF report"
                ),
            }],
        })

        # Slack's hard limit is 50 blocks — trim from the middle if needed
        if len(blocks) > 50:
            blocks = blocks[:48] + [
                {"type": "divider"},
                {"type": "context", "elements": [{"type": "mrkdwn", "text": "_Some detail sections were omitted — see the PDF for the full report._"}]},
            ]

        return blocks
