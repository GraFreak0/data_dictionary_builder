"""
Notifications module — email and Slack report delivery.
"""

from .email_sender import EmailSender
from .slack_notifier import SlackNotifier

__all__ = [
    "EmailSender",
    "SlackNotifier",
]
