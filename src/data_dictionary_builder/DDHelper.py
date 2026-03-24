"""
DDHelper
========
High-level helper bundled with data_dictionary_builder.

Provides the standard output-directory layout, JSON report persistence,
PDF compilation from JSON reports, and email delivery with the PDF
attached — all in one importable class.

Directory layout created automatically
---------------------------------------
    <base>/
        models/              ← YAML files  ({schema}.yml, all_models.yml)
        reports/
            json/            ← Metadata_comparison_{datetime}.json
            pdf/             ← Metadata_comparison_{datetime}.pdf

Usage
-----
    from data_dictionary_builder import DDHelper

    helper = DDHelper()                              # base = current working directory
    helper = DDHelper(base_dir="/data")              # custom root
    helper = DDHelper(models_dir="/data/my_models",  # discrete absolute paths
                      reports_dir="/data/my_reports")

    dirs = helper.dirs
    # dirs["models"]       → Path("<base>/models")
    # dirs["reports_json"] → Path("<base>/reports/json")
    # dirs["reports_pdf"]  → Path("<base>/reports/pdf")

    # Save a comparison report (returns the json Path)
    json_path = helper.save_report(report)

    # Compile all JSON reports in reports/json/ into one PDF
    pdf_path = helper.compile_pdf()

    # Or compile only the most-recently saved one
    pdf_path = helper.compile_pdf(source_json=json_path)

    # Send notification — email, Slack, or both
    helper.send_notification(
        notification_type="email",   # "email" | "slack" | "both"
        report=report,
        pdf_path=pdf_path,
        subject="Nightly schema drift — analytics",
        email_to="team@example.com",           # or set EMAIL_TO env var
        slack_target="#data-alerts",           # or set SLACK_NOTIFY_TARGET env var
    )

    # Legacy: send email only (unchanged API)
    helper.send_report_email(
        report=report,
        pdf_path=pdf_path,
        subject="Nightly schema drift — analytics",
    )
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union

logger = logging.getLogger(__name__)


class DDHelper:
    """
    Utility class that manages output directories, serialises comparison
    reports to JSON, compiles them into a PDF, and sends the PDF by email.

    Parameters
    ----------
    base_dir : str | Path
        Root directory under which ``models/``, ``reports/json/``, and
        ``reports/pdf/`` will be created.  Defaults to the current working
        directory.
    models_dir : str | Path, optional
        Explicit path for the models directory. If not provided,
        defaults to ``base_dir / "models"``.
    reports_dir : str | Path, optional
        Explicit path for the reports directory. If not provided,
        defaults to ``base_dir / "reports"``.
    """

    # ------------------------------------------------------------------ #
    # Construction                                                         #
    # ------------------------------------------------------------------ #

    def __init__(
        self,
        base_dir: Union[str, Path] = ".",
        models_dir: Optional[Union[str, Path]] = None,
        reports_dir: Optional[Union[str, Path]] = None,
    ) -> None:
        self._base = Path(base_dir).resolve()
        self._models_dir = Path(models_dir).resolve() if models_dir else self._base / "models"
        self._reports_dir = Path(reports_dir).resolve() if reports_dir else self._base / "reports"
        self._dirs = self._create_dirs()

        logger.info(
            "DDHelper initialised — base: %s  |  dirs: %s",
            self._base,
            {k: str(v) for k, v in self._dirs.items()},
        )

    # ------------------------------------------------------------------ #
    # Public properties                                                    #
    # ------------------------------------------------------------------ #

    @property
    def base_dir(self) -> Path:
        """Root directory passed to the constructor."""
        return self._base

    @property
    def dirs(self) -> Dict[str, Path]:
        """
        Dictionary of the four managed output directories.

        Keys
        ----
        ``models``       – YAML output directory
        ``reports``      – reports root
        ``reports_json`` – JSON report files
        ``reports_pdf``  – PDF report files
        """
        return self._dirs

    # Convenience shortcuts ---------------------------------------------------

    @property
    def models_dir(self) -> Path:
        return self._dirs["models"]

    @property
    def reports_json_dir(self) -> Path:
        return self._dirs["reports_json"]

    @property
    def reports_pdf_dir(self) -> Path:
        return self._dirs["reports_pdf"]

    # ------------------------------------------------------------------ #
    # Directory management                                                 #
    # ------------------------------------------------------------------ #

    def _create_dirs(self) -> Dict[str, Path]:
        dirs = {
            "models":       self._models_dir,
            "reports":      self._reports_dir,
            "reports_json": self._reports_dir / "json",
            "reports_pdf":  self._reports_dir / "pdf",
        }
        for path in dirs.values():
            path.mkdir(parents=True, exist_ok=True)
        return dirs

    # ------------------------------------------------------------------ #
    # Report filename helpers                                              #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _report_stem(dt: Optional[datetime] = None) -> str:
        """Return  ``Metadata_comparison_YYYY-MM-DD_HH-MM-SS``."""
        ts = (dt or datetime.now()).strftime("%Y-%m-%d_%H-%M-%S")
        return f"Metadata_comparison_{ts}"

    def report_json_path(self, dt: Optional[datetime] = None) -> Path:
        """Full path for a new JSON report file."""
        return self._dirs["reports_json"] / f"{self._report_stem(dt)}.json"

    def report_pdf_path(self, stem: Optional[str] = None, dt: Optional[datetime] = None) -> Path:
        """
        Full path for a PDF report file.

        Parameters
        ----------
        stem : str, optional
            Use an explicit stem (e.g. taken from the source JSON filename).
            When omitted a new timestamp-based stem is generated.
        """
        s = stem or self._report_stem(dt)
        return self._dirs["reports_pdf"] / f"{s}.pdf"

    # ------------------------------------------------------------------ #
    # JSON persistence                                                     #
    # ------------------------------------------------------------------ #

    def save_report(
        self,
        report: dict,
        dt: Optional[datetime] = None,
    ) -> Path:
        """
        Serialise *report* as pretty-printed JSON inside ``reports/json/``.

        The filename is ``Metadata_comparison_{datetime}.json``.

        Parameters
        ----------
        report : dict
            The comparison report dict returned by
            ``SchemaComparator.compare_and_generate_report()``.
        dt : datetime, optional
            Override the timestamp used in the filename.  Defaults to now.

        Returns
        -------
        Path
            Full path of the saved JSON file.
        """
        path = self.report_json_path(dt)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2, default=str)
        logger.info("JSON report saved: %s  (%d bytes)", path, path.stat().st_size)
        return path

    # ------------------------------------------------------------------ #
    # PDF compilation                                                      #
    # ------------------------------------------------------------------ #

    def compile_pdf(
        self,
        source_json: Optional[Path] = None,
        output_pdf: Optional[Path] = None,
    ) -> Optional[Path]:
        """
        Compile one or all JSON reports in ``reports/json/`` into a PDF
        saved in ``reports/pdf/``.

        Parameters
        ----------
        source_json : Path, optional
            Compile only this specific JSON file.  When omitted, **all**
            ``*.json`` files in ``reports/json/`` are compiled into a single
            PDF (sorted alphabetically, newest last due to the timestamp
            naming convention).
        output_pdf : Path, optional
            Override the output PDF path.  Defaults to a file in
            ``reports/pdf/`` whose stem matches the source JSON (single-file
            mode) or a fresh timestamp (multi-file mode).

        Returns
        -------
        Path | None
            Path of the generated PDF, or ``None`` when no JSON files were
            found or reportlab is not installed.
        """
        try:
            from reportlab.lib import colors
            from reportlab.lib.pagesizes import LETTER
            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            from reportlab.lib.units import inch
            from reportlab.platypus import (
                HRFlowable,
                PageBreak,
                Paragraph,
                SimpleDocTemplate,
                Spacer,
                Table,
                TableStyle,
            )
        except ImportError:
            logger.error(
                "reportlab is not installed — cannot compile PDF. "
                "Install it with:  pip install reportlab"
            )
            return None

        # ── Collect source files ──────────────────────────────────────────
        if source_json is not None:
            json_files = [Path(source_json)]
        else:
            json_files = sorted(self._dirs["reports_json"].glob("*.json"))

        if not json_files:
            logger.warning("No JSON reports found — skipping PDF compilation")
            return None

        # ── Determine output path ─────────────────────────────────────────
        if output_pdf is not None:
            pdf_path = Path(output_pdf)
        elif source_json is not None:
            pdf_path = self.report_pdf_path(stem=Path(source_json).stem)
        else:
            pdf_path = self.report_pdf_path()

        # ── ReportLab setup ───────────────────────────────────────────────
        doc = SimpleDocTemplate(
            str(pdf_path),
            pagesize=LETTER,
            leftMargin=0.75 * inch,
            rightMargin=0.75 * inch,
            topMargin=0.85 * inch,
            bottomMargin=0.85 * inch,
        )

        styles = getSampleStyleSheet()

        NAVY   = colors.HexColor("#1B2A4A")
        BLUE   = colors.HexColor("#2E5FA3")
        TEAL   = colors.HexColor("#1A7A78")
        SILVER = colors.HexColor("#F2F5F9")
        LGRAY  = colors.HexColor("#D0D9E8")

        title_style = ParagraphStyle(
            "DDTitle", parent=styles["Title"],
            fontSize=20, spaceAfter=6, textColor=NAVY,
        )
        h1_style = ParagraphStyle(
            "DDH1", parent=styles["Heading1"],
            fontSize=14, spaceBefore=20, spaceAfter=4, textColor=BLUE,
        )
        h2_style = ParagraphStyle(
            "DDH2", parent=styles["Heading2"],
            fontSize=11, spaceBefore=12, spaceAfter=3, textColor=TEAL,
        )
        body_style = ParagraphStyle(
            "DDBody", parent=styles["Normal"],
            fontSize=9, spaceAfter=3, leading=13,
        )
        # Cell style: wraps long values within their column boundary.
        # splitLongWords ensures that even strings without spaces (e.g. long
        # identifiers) break at the column edge rather than overflowing.
        cell_style = ParagraphStyle(
            "DDCell", parent=styles["Normal"],
            fontSize=8, leading=11, splitLongWords=1, wordWrap="LTR",
        )

        def _c(text: str) -> Paragraph:
            """Wrap a cell value so reportlab word-wraps it within its column."""
            return Paragraph(str(text), cell_style)

        def _hdr_style() -> TableStyle:
            return TableStyle([
                ("BACKGROUND",     (0, 0), (-1, 0),  NAVY),
                ("TEXTCOLOR",      (0, 0), (-1, 0),  colors.white),
                ("FONTNAME",       (0, 0), (-1, 0),  "Helvetica-Bold"),
                ("FONTSIZE",       (0, 0), (-1, 0),  9),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, SILVER]),
                ("FONTSIZE",       (0, 1), (-1, -1), 9),
                ("GRID",           (0, 0), (-1, -1), 0.5, LGRAY),
                ("VALIGN",         (0, 0), (-1, -1), "TOP"),
                ("TOPPADDING",     (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING",  (0, 0), (-1, -1), 5),
                ("LEFTPADDING",    (0, 0), (-1, -1), 8),
                ("RIGHTPADDING",   (0, 0), (-1, -1), 8),
            ])

        def _sub_style() -> TableStyle:
            return TableStyle([
                ("BACKGROUND",     (0, 0), (-1, 0),  BLUE),
                ("TEXTCOLOR",      (0, 0), (-1, 0),  colors.white),
                ("FONTNAME",       (0, 0), (-1, 0),  "Helvetica-Bold"),
                ("FONTSIZE",       (0, 0), (-1, 0),  8),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, SILVER]),
                ("FONTSIZE",       (0, 1), (-1, -1), 8),
                ("GRID",           (0, 0), (-1, -1), 0.5, LGRAY),
                ("VALIGN",         (0, 0), (-1, -1), "TOP"),
                ("TOPPADDING",     (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING",  (0, 0), (-1, -1), 4),
                ("LEFTPADDING",    (0, 0), (-1, -1), 6),
                ("RIGHTPADDING",   (0, 0), (-1, -1), 6),
            ])

        W = LETTER[0] - 1.5 * inch  # usable content width

        story: list = []

        # ── Cover page ────────────────────────────────────────────────────
        story.append(Spacer(1, 0.35 * inch))
        story.append(Paragraph("Schema Comparison Reports", title_style))
        story.append(Paragraph(
            f"Generated {datetime.now().strftime('%Y-%m-%d  %H:%M:%S')}  "
            f"—  {len(json_files)} report(s)",
            body_style,
        ))
        story.append(HRFlowable(width="100%", thickness=2, color=NAVY, spaceAfter=16))

        # Table of contents
        story.append(Paragraph("Contents", h1_style))
        for i, jf in enumerate(json_files, 1):
            story.append(Paragraph(f"{i}.  {jf.stem}", body_style))
        story.append(PageBreak())

        # ── One section per JSON file ─────────────────────────────────────
        for jf in json_files:
            try:
                report = json.loads(jf.read_text(encoding="utf-8"))
            except Exception as exc:
                story.append(Paragraph(f"⚠  Could not parse {jf.name}: {exc}", body_style))
                continue

            summary    = report.get("summary", {})
            comparison = report.get("comparison", {})
            yaml_gaps  = report.get("yaml_gaps", {})

            # Section header
            story.append(Paragraph(jf.stem, h1_style))
            story.append(Paragraph(
                f"File: {jf.name}  |  "
                f"Modified: {datetime.fromtimestamp(jf.stat().st_mtime).strftime('%Y-%m-%d  %H:%M')}",
                body_style,
            ))
            story.append(HRFlowable(width="100%", thickness=1, color=BLUE, spaceAfter=10))

            # Summary table
            story.append(Paragraph("Summary", h2_style))
            sum_rows = [["Metric", "Count"]] + [
                [_c(label), _c(str(summary.get(key, 0)))]
                for label, key in [
                    ("Missing tables",              "missing_tables_count"),
                    ("Missing columns",             "missing_columns_count"),
                    ("Type mismatches",             "type_mismatches_count"),
                    ("Tables without description",  "tables_without_descriptions_count"),
                    ("Columns without description", "columns_without_descriptions_count"),
                ]
            ]
            t = Table(sum_rows, colWidths=[W * 0.75, W * 0.25], repeatRows=1)
            t.setStyle(_hdr_style())
            story.append(t)

            # Missing tables — all rows, no truncation
            missing_tables = comparison.get("missing_tables", [])
            if missing_tables:
                story.append(Paragraph(f"Missing Tables ({len(missing_tables)})", h2_style))
                rows = [["Schema", "Table"]] + [
                    [_c(r.get("schema", "")), _c(r.get("table", ""))]
                    for r in missing_tables
                ]
                t = Table(rows, colWidths=[W * 0.35, W * 0.65], repeatRows=1)
                t.setStyle(_sub_style())
                story.append(t)

            # Missing columns — all rows, no truncation
            missing_cols = comparison.get("missing_columns", [])
            if missing_cols:
                story.append(Paragraph(f"Missing Columns ({len(missing_cols)})", h2_style))
                rows = [["Schema", "Table", "Column", "Expected Type"]] + [
                    [_c(r.get("schema", "")), _c(r.get("table", "")),
                     _c(r.get("column", "")), _c(r.get("data_type", ""))]
                    for r in missing_cols
                ]
                t = Table(rows, colWidths=[W*0.18, W*0.25, W*0.32, W*0.25], repeatRows=1)
                t.setStyle(_sub_style())
                story.append(t)

            # Type mismatches — all rows, no truncation
            type_mm = comparison.get("type_mismatches", [])
            if type_mm:
                story.append(Paragraph(f"Type Mismatches ({len(type_mm)})", h2_style))
                rows = [["Schema", "Table", "Column", "Source", "Destination"]] + [
                    [_c(r.get("schema", "")), _c(r.get("table", "")),
                     _c(r.get("column", "")), _c(r.get("source_type", "")),
                     _c(r.get("destination_type", ""))]
                    for r in type_mm
                ]
                t = Table(rows, colWidths=[W*0.14, W*0.2, W*0.26, W*0.2, W*0.2], repeatRows=1)
                t.setStyle(_sub_style())
                story.append(t)

            # Documentation gaps — tables, all rows, no truncation
            tbl_gaps = yaml_gaps.get("tables_without_descriptions", [])
            if tbl_gaps:
                story.append(Paragraph(f"Tables Without Descriptions ({len(tbl_gaps)})", h2_style))
                rows = [["Schema.Table"]] + [[_c(g)] for g in tbl_gaps]
                t = Table(rows, colWidths=[W], repeatRows=1)
                t.setStyle(_sub_style())
                story.append(t)

            # Documentation gaps — columns, all rows, no truncation
            col_gaps = yaml_gaps.get("columns_without_descriptions", [])
            if col_gaps:
                story.append(Paragraph(f"Columns Without Descriptions ({len(col_gaps)})", h2_style))
                rows = [["Schema", "Table", "Column"]] + [
                    [_c(g.get("schema", "")), _c(g.get("table", "")), _c(g.get("column", ""))]
                    for g in col_gaps
                ]
                t = Table(rows, colWidths=[W*0.25, W*0.35, W*0.40], repeatRows=1)
                t.setStyle(_sub_style())
                story.append(t)

            story.append(Spacer(1, 0.25 * inch))
            story.append(PageBreak())

        # Remove trailing page break
        if story and isinstance(story[-1], PageBreak):
            story.pop()

        doc.build(story)
        logger.info("PDF compiled: %s  (%d bytes)", pdf_path, pdf_path.stat().st_size)
        return pdf_path

    # ------------------------------------------------------------------ #
    # Email delivery                                                        #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _parse_csv_list(value) -> List[str]:
        """Normalise a comma-separated string, a list, or None into a clean list of strings."""
        if not value:
            return []
        if isinstance(value, (list, tuple)):
            return [str(v).strip() for v in value if str(v).strip()]
        return [v.strip() for v in str(value).split(",") if v.strip()]

    def send_report_email(
        self,
        report: dict,
        pdf_path: Optional[Path] = None,
        subject: Optional[str] = None,
        *,
        smtp_host: Optional[str] = None,
        smtp_port: Optional[int] = None,
        smtp_user: Optional[str] = None,
        smtp_password: Optional[str] = None,
        email_to=None,
        use_tls: bool = True,
    ) -> bool:
        """
        Send a comparison report email with the compiled PDF attached.

        All SMTP parameters fall back to environment variables when not
        supplied explicitly:

        ============  ===================
        Parameter     Env var
        ============  ===================
        smtp_host     ``SMTP_HOST``
        smtp_port     ``SMTP_PORT``
        smtp_user     ``SMTP_USER``
        smtp_password ``SMTP_PASSWORD``
        email_to      ``EMAIL_TO``
        ============  ===================

        Parameters
        ----------
        report : dict
            Comparison report to include in the email body.
        pdf_path : Path, optional
            PDF file to attach.  Skipped gracefully if ``None`` or if the
            file does not exist.
        subject : str, optional
            Email subject line.  Defaults to
            ``"Database Schema Comparison Report"``.
        smtp_host, smtp_port, smtp_user, smtp_password, email_to
            SMTP credentials.  All fall back to env vars (see table above).
        use_tls : bool
            Whether to use STARTTLS.  Defaults to ``True``.

        Returns
        -------
        bool
            ``True`` if the email was dispatched successfully.
        """
        smtp_host     = smtp_host     or os.getenv("SMTP_HOST")
        smtp_port     = smtp_port     or int(os.getenv("SMTP_PORT", 587))
        smtp_user     = smtp_user     or os.getenv("SMTP_USER", "")
        smtp_password = smtp_password or os.getenv("SMTP_PASSWORD")

        # Accept a list, a comma-separated string, or a bare address.
        recipients: List[str] = self._parse_csv_list(email_to or os.getenv("EMAIL_TO", ""))

        if not smtp_host or not recipients:
            logger.info(
                "Email skipped — set SMTP_HOST and EMAIL_TO environment "
                "variables (or pass them explicitly) to enable email delivery"
            )
            return False

        from .notifications.email_sender import EmailSender  # type: ignore[import]

        # Strip spaces from app passwords (e.g. Gmail displays them as
        # "xxxx xxxx xxxx xxxx" but the actual credential has no spaces).
        if smtp_password:
            smtp_password = smtp_password.replace(" ", "")

        # Port 465 uses implicit SSL (SMTP_SSL); port 587/25 use STARTTLS.
        use_ssl = int(smtp_port) == 465

        sender = EmailSender(
            smtp_host=smtp_host,
            smtp_port=smtp_port,
            sender_email=smtp_user,
            sender_password=smtp_password,
            use_tls=use_tls and not use_ssl,
            use_ssl=use_ssl,
            timeout=15,
        )

        attachments: List[str] = []
        if pdf_path is not None and Path(pdf_path).exists():
            attachments.append(str(pdf_path))

        ok = sender.send_comparison_report(
            recipient_emails=recipients,
            report=report,
            subject=subject or "Database Schema Comparison Report",
            attachments=attachments,
        )

        logger.info(
            "Email sent=%s  to=%s  attachments=%s",
            ok, recipients, attachments,
        )
        return ok

    # ------------------------------------------------------------------ #
    # Unified notification delivery                                        #
    # ------------------------------------------------------------------ #

    def send_notification(
        self,
        notification_type: str,
        report: dict,
        pdf_path: Optional[Path] = None,
        subject: Optional[str] = None,
        *,
        # ── Email params ─────────────────────────────────────────────────
        smtp_host: Optional[str] = None,
        smtp_port: Optional[int] = None,
        smtp_user: Optional[str] = None,
        smtp_password: Optional[str] = None,
        email_to=None,           # str, comma-separated str, or List[str]
        use_tls: bool = True,
        # ── Slack params ─────────────────────────────────────────────────
        slack_token: Optional[str] = None,
        slack_target=None,       # str, comma-separated str, or List[str]
        slack_pipeline_label: Optional[str] = None,
    ) -> Dict[str, bool]:
        """
        Send a comparison report via one or more notification channels.

        Parameters
        ----------
        notification_type : str
            ``"email"`` — email only.
            ``"slack"`` — Slack only.
            ``"both"``  — email **and** Slack.
        report : dict
            Comparison report dict (from ``SchemaComparator`` or built manually).
        pdf_path : Path, optional
            PDF to attach (email) or upload (Slack).  Skipped when ``None``.
        subject : str, optional
            Email subject / Slack message title.
        smtp_host, smtp_port, smtp_user, smtp_password, email_to, use_tls
            Email / SMTP credentials.  All fall back to their corresponding
            ``SMTP_*`` / ``EMAIL_TO`` environment variables.
        slack_token : str, optional
            Slack Bot Token (``xoxb-…``).  Falls back to ``SLACK_BOT_TOKEN``.
        slack_target : str, optional
            Destination: ``"#channel"``, ``"@username"``, ``"C…"`` (channel ID),
            or ``"U…"`` (user ID).  Falls back to ``SLACK_NOTIFY_TARGET``.
        slack_pipeline_label : str, optional
            Optional label shown in the Slack Block Kit message header.

        Returns
        -------
        dict
            ``{"email": bool, "slack": bool}`` — ``True`` when delivered
            successfully.  Channels not requested carry ``False``.

        Raises
        ------
        ValueError
            When *notification_type* is not one of the three accepted values.
        """
        nt = notification_type.lower().strip()
        if nt not in ("email", "slack", "both"):
            raise ValueError(
                "notification_type must be 'email', 'slack', or 'both'; "
                f"got {notification_type!r}"
            )

        results: Dict[str, bool] = {"email": False, "slack": False}

        # ── Email ─────────────────────────────────────────────────────────
        if nt in ("email", "both"):
            results["email"] = self.send_report_email(
                report=report,
                pdf_path=pdf_path,
                subject=subject,
                smtp_host=smtp_host,
                smtp_port=smtp_port,
                smtp_user=smtp_user,
                smtp_password=smtp_password,
                email_to=email_to,
                use_tls=use_tls,
            )

        # ── Slack ─────────────────────────────────────────────────────────
        if nt in ("slack", "both"):
            slack_token   = slack_token or os.getenv("SLACK_BOT_TOKEN")
            slack_targets = self._parse_csv_list(slack_target or os.getenv("SLACK_NOTIFY_TARGET", ""))

            if not slack_token or not slack_targets:
                logger.info(
                    "Slack skipped — set SLACK_BOT_TOKEN and SLACK_NOTIFY_TARGET "
                    "environment variables (or pass them explicitly) to enable "
                    "Slack delivery"
                )
            else:
                from .notifications.slack_notifier import SlackNotifier  # type: ignore[import]

                try:
                    notifier = SlackNotifier(token=slack_token)
                except ValueError as exc:
                    logger.error("Slack skipped — invalid token: %s", exc)
                else:
                    target_results: List[bool] = []
                    for _target in slack_targets:
                        ok = notifier.send_comparison_report(
                            target=_target,
                            report=report,
                            pdf_path=pdf_path,
                            title=subject or "Database Schema Comparison Report",
                            pipeline_label=slack_pipeline_label,
                        )
                        target_results.append(ok)
                        logger.info("Slack sent=%s  to=%s", ok, _target)
                    results["slack"] = all(target_results)

        return results

    # ------------------------------------------------------------------ #
    # Dunder                                                               #
    # ------------------------------------------------------------------ #

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"DDHelper(base_dir={str(self._base)!r}, "
            f"models={str(self._dirs['models'])!r}, "
            f"reports_json={str(self._dirs['reports_json'])!r}, "
            f"reports_pdf={str(self._dirs['reports_pdf'])!r})"
        )
