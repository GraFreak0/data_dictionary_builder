"""
Email sender for sending schema comparison reports.
"""

import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, Any, List, Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EmailSender:
    """Sends email notifications with comparison reports."""
    
    def __init__(
        self,
        smtp_host: str,
        smtp_port: int,
        sender_email: str,
        sender_password: Optional[str] = None,
        use_tls: bool = True,
        use_ssl: bool = False
    ):
        """
        Initialize the email sender.
        
        Args:
            smtp_host: SMTP server host
            smtp_port: SMTP server port
            sender_email: Email address to send from
            sender_password: Password for sender email (optional for some SMTP servers)
            use_tls: Whether to use TLS (default: True)
            use_ssl: Whether to use SSL (default: False)
        """
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.sender_email = sender_email
        self.sender_password = sender_password
        self.use_tls = use_tls
        self.use_ssl = use_ssl
    
    def send_comparison_report(
        self,
        recipient_emails: List[str],
        report: Dict[str, Any],
        subject: Optional[str] = None
    ) -> bool:
        """
        Send a schema comparison report via email.
        
        Args:
            recipient_emails: List of recipient email addresses
            report: Comparison report dictionary
            subject: Optional custom email subject
            
        Returns:
            True if email sent successfully, False otherwise
        """
        if subject is None:
            subject = "Database Schema Comparison Report"
        
        # Generate email body
        html_body = self._generate_html_report(report)
        text_body = self._generate_text_report(report)
        
        # Send email
        return self.send_email(
            recipient_emails=recipient_emails,
            subject=subject,
            text_body=text_body,
            html_body=html_body
        )
    
    def send_email(
        self,
        recipient_emails: List[str],
        subject: str,
        text_body: str,
        html_body: Optional[str] = None
    ) -> bool:
        """
        Send an email.
        
        Args:
            recipient_emails: List of recipient email addresses
            subject: Email subject
            text_body: Plain text email body
            html_body: Optional HTML email body
            
        Returns:
            True if email sent successfully, False otherwise
        """
        try:
            # Create message
            message = MIMEMultipart('alternative')
            message['Subject'] = subject
            message['From'] = self.sender_email
            message['To'] = ', '.join(recipient_emails)
            
            # Add text part
            text_part = MIMEText(text_body, 'plain')
            message.attach(text_part)
            
            # Add HTML part if provided
            if html_body:
                html_part = MIMEText(html_body, 'html')
                message.attach(html_part)
            
            # Connect to SMTP server and send
            if self.use_ssl:
                server = smtplib.SMTP_SSL(self.smtp_host, self.smtp_port)
            else:
                server = smtplib.SMTP(self.smtp_host, self.smtp_port)
                if self.use_tls:
                    server.starttls()
            
            # Login if password provided
            if self.sender_password:
                server.login(self.sender_email, self.sender_password)
            
            # Send email
            server.sendmail(self.sender_email, recipient_emails, message.as_string())
            server.quit()
            
            logger.info(f"Email sent successfully to {', '.join(recipient_emails)}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to send email: {e}")
            return False
    
    def _generate_html_report(self, report: Dict[str, Any]) -> str:
        """
        Generate HTML email body from comparison report.
        
        Args:
            report: Comparison report dictionary
            
        Returns:
            HTML string
        """
        summary = report.get('summary', {})
        comparison = report.get('comparison', {})
        yaml_gaps = report.get('yaml_gaps', {})
        
        html = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; }}
                h1 {{ color: #333; }}
                h2 {{ color: #666; }}
                table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #4CAF50; color: white; }}
                tr:nth-child(even) {{ background-color: #f2f2f2; }}
                .summary {{ background-color: #e7f3fe; padding: 15px; margin-bottom: 20px; border-left: 6px solid #2196F3; }}
                .warning {{ color: #ff9800; }}
                .error {{ color: #f44336; }}
            </style>
        </head>
        <body>
            <h1>Database Schema Comparison Report</h1>
            
            <div class="summary">
                <h2>Summary</h2>
                <p><strong>Missing Tables:</strong> {summary.get('missing_tables_count', 0)}</p>
                <p><strong>Missing Columns:</strong> {summary.get('missing_columns_count', 0)}</p>
                <p><strong>Type Mismatches:</strong> {summary.get('type_mismatches_count', 0)}</p>
                <p><strong>Tables Without Descriptions:</strong> {summary.get('tables_without_descriptions_count', 0)}</p>
                <p><strong>Columns Without Descriptions:</strong> {summary.get('columns_without_descriptions_count', 0)}</p>
            </div>
        """
        
        # Missing tables
        if comparison.get('missing_tables'):
            html += "<h2>Missing Tables in Destination</h2>"
            html += "<table><tr><th>Schema</th><th>Table</th><th>Column Count</th></tr>"
            for table in comparison['missing_tables']:
                html += f"<tr><td>{table['schema']}</td><td>{table['table']}</td><td>{table['column_count']}</td></tr>"
            html += "</table>"
        
        # Missing columns
        if comparison.get('missing_columns'):
            html += "<h2>Missing Columns in Destination</h2>"
            html += "<table><tr><th>Schema</th><th>Table</th><th>Column</th><th>Data Type</th></tr>"
            for col in comparison['missing_columns'][:50]:  # Limit to first 50
                html += f"<tr><td>{col['schema']}</td><td>{col['table']}</td><td>{col['column']}</td><td>{col['data_type']}</td></tr>"
            html += "</table>"
            if len(comparison['missing_columns']) > 50:
                html += f"<p><em>... and {len(comparison['missing_columns']) - 50} more columns</em></p>"
        
        # Type mismatches
        if comparison.get('type_mismatches'):
            html += "<h2>Data Type Mismatches</h2>"
            html += "<table><tr><th>Schema</th><th>Table</th><th>Column</th><th>Source Type</th><th>Destination Type</th></tr>"
            for mismatch in comparison['type_mismatches']:
                html += f"<tr><td>{mismatch['schema']}</td><td>{mismatch['table']}</td><td>{mismatch['column']}</td><td>{mismatch['source_type']}</td><td>{mismatch['destination_type']}</td></tr>"
            html += "</table>"
        
        # Tables without descriptions
        if yaml_gaps and yaml_gaps.get('tables_without_descriptions'):
            html += "<h2>Tables Without Descriptions</h2>"
            html += "<ul>"
            for table in yaml_gaps['tables_without_descriptions'][:20]:  # Limit to first 20
                html += f"<li>{table}</li>"
            html += "</ul>"
            if len(yaml_gaps['tables_without_descriptions']) > 20:
                html += f"<p><em>... and {len(yaml_gaps['tables_without_descriptions']) - 20} more tables</em></p>"
        
        # Columns without descriptions
        if yaml_gaps and yaml_gaps.get('columns_without_descriptions'):
            html += "<h2>Columns Without Descriptions (Sample)</h2>"
            html += "<table><tr><th>Schema</th><th>Table</th><th>Column</th></tr>"
            for col in yaml_gaps['columns_without_descriptions'][:30]:  # Limit to first 30
                html += f"<tr><td>{col['schema']}</td><td>{col['table']}</td><td>{col['column']}</td></tr>"
            html += "</table>"
            if len(yaml_gaps['columns_without_descriptions']) > 30:
                html += f"<p><em>... and {len(yaml_gaps['columns_without_descriptions']) - 30} more columns</em></p>"
        
        html += """
        </body>
        </html>
        """
        
        return html
    
    def _generate_text_report(self, report: Dict[str, Any]) -> str:
        """
        Generate plain text email body from comparison report.
        
        Args:
            report: Comparison report dictionary
            
        Returns:
            Plain text string
        """
        summary = report.get('summary', {})
        comparison = report.get('comparison', {})
        
        text = "DATABASE SCHEMA COMPARISON REPORT\n"
        text += "=" * 50 + "\n\n"
        
        text += "SUMMARY\n"
        text += "-" * 50 + "\n"
        text += f"Missing Tables: {summary.get('missing_tables_count', 0)}\n"
        text += f"Missing Columns: {summary.get('missing_columns_count', 0)}\n"
        text += f"Type Mismatches: {summary.get('type_mismatches_count', 0)}\n"
        text += f"Tables Without Descriptions: {summary.get('tables_without_descriptions_count', 0)}\n"
        text += f"Columns Without Descriptions: {summary.get('columns_without_descriptions_count', 0)}\n\n"
        
        if comparison.get('missing_tables'):
            text += "MISSING TABLES IN DESTINATION\n"
            text += "-" * 50 + "\n"
            for table in comparison['missing_tables']:
                text += f"  - {table['schema']}.{table['table']} ({table['column_count']} columns)\n"
            text += "\n"
        
        if comparison.get('missing_columns'):
            text += "MISSING COLUMNS IN DESTINATION (First 20)\n"
            text += "-" * 50 + "\n"
            for col in comparison['missing_columns'][:20]:
                text += f"  - {col['schema']}.{col['table']}.{col['column']} ({col['data_type']})\n"
            if len(comparison['missing_columns']) > 20:
                text += f"  ... and {len(comparison['missing_columns']) - 20} more columns\n"
            text += "\n"
        
        text += "For full details, please see the HTML version of this email.\n"
        
        return text
