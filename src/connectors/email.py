"""
Email Connector

Send emails via SMTP or email service APIs.
"""

import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Any
from .base import BaseConnector, ConnectorResult


class EmailConnector(BaseConnector):
    """Connector for sending emails."""

    service_name = "email"
    display_name = "Email"

    def get_actions(self) -> list[dict[str, str]]:
        return [
            {"action": "send", "description": "Send an email"},
            {"action": "send_html", "description": "Send an HTML email"},
            {"action": "send_template", "description": "Send an email using a template"},
        ]

    def validate_credentials(self) -> bool:
        # SMTP requires host, port, username, password
        # API services require api_key
        return (
            ("smtp_host" in self.credentials and "smtp_user" in self.credentials)
            or "api_key" in self.credentials
            or "sendgrid_key" in self.credentials
            or "mailgun_key" in self.credentials
        )

    async def execute(self, action: str, inputs: dict[str, Any]) -> ConnectorResult:
        """Execute an email action."""
        if action == "send":
            return await self._send_email(inputs, html=False)
        elif action == "send_html":
            return await self._send_email(inputs, html=True)
        elif action == "send_template":
            return await self._send_template(inputs)
        else:
            return ConnectorResult(success=False, error=f"Unknown action: {action}")

    async def _send_email(self, inputs: dict[str, Any], html: bool = False) -> ConnectorResult:
        """Send an email."""
        to_email = inputs.get("to", "")
        subject = inputs.get("subject", "")
        body = inputs.get("body", inputs.get("message", ""))
        from_email = inputs.get("from", self.credentials.get("from_email", ""))

        if not to_email or not subject or not body:
            return ConnectorResult(
                success=False, error="To, subject, and body are required"
            )

        # Check which method to use
        if self.credentials.get("sendgrid_key"):
            return await self._send_via_sendgrid(to_email, from_email, subject, body, html)
        elif self.credentials.get("mailgun_key"):
            return await self._send_via_mailgun(to_email, from_email, subject, body, html)
        elif self.credentials.get("smtp_host"):
            return await self._send_via_smtp(to_email, from_email, subject, body, html)
        else:
            return ConnectorResult(
                success=False, error="No email credentials configured"
            )

    async def _send_via_smtp(
        self, to: str, from_email: str, subject: str, body: str, html: bool
    ) -> ConnectorResult:
        """Send email via SMTP."""
        try:
            smtp_host = self.credentials.get("smtp_host", "")
            smtp_port = int(self.credentials.get("smtp_port", 587))
            smtp_user = self.credentials.get("smtp_user", "")
            smtp_pass = self.credentials.get("smtp_pass", "")
            use_tls = self.credentials.get("use_tls", True)

            # Create message
            if html:
                msg = MIMEMultipart("alternative")
                msg.attach(MIMEText(body, "html"))
            else:
                msg = MIMEText(body)

            msg["Subject"] = subject
            msg["From"] = from_email or smtp_user
            msg["To"] = to

            # Send
            context = ssl.create_default_context()

            if use_tls:
                server = smtplib.SMTP(smtp_host, smtp_port)
                server.starttls(context=context)
            else:
                server = smtplib.SMTP_SSL(smtp_host, smtp_port, context=context)

            server.login(smtp_user, smtp_pass)
            server.sendmail(from_email or smtp_user, to.split(","), msg.as_string())
            server.quit()

            return ConnectorResult(
                success=True,
                data={"message": "Email sent successfully", "to": to},
            )

        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    async def _send_via_sendgrid(
        self, to: str, from_email: str, subject: str, body: str, html: bool
    ) -> ConnectorResult:
        """Send email via SendGrid API."""
        api_key = self.credentials.get("sendgrid_key", "")

        payload = {
            "personalizations": [{"to": [{"email": to}]}],
            "from": {"email": from_email},
            "subject": subject,
            "content": [
                {
                    "type": "text/html" if html else "text/plain",
                    "value": body,
                }
            ],
        }

        return await self._request(
            "POST",
            "https://api.sendgrid.com/v3/mail/send",
            headers={"Authorization": f"Bearer {api_key}"},
            json=payload,
        )

    async def _send_via_mailgun(
        self, to: str, from_email: str, subject: str, body: str, html: bool
    ) -> ConnectorResult:
        """Send email via Mailgun API."""
        api_key = self.credentials.get("mailgun_key", "")
        domain = self.credentials.get("mailgun_domain", "")

        if not domain:
            return ConnectorResult(success=False, error="Mailgun domain is required")

        import base64
        auth = base64.b64encode(f"api:{api_key}".encode()).decode()

        data = {
            "from": from_email,
            "to": to,
            "subject": subject,
        }

        if html:
            data["html"] = body
        else:
            data["text"] = body

        return await self._request(
            "POST",
            f"https://api.mailgun.net/v3/{domain}/messages",
            headers={"Authorization": f"Basic {auth}"},
            data=data,
        )

    async def _send_template(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Send an email using a template."""
        to_email = inputs.get("to", "")
        template_id = inputs.get("template_id", "")
        template_data = inputs.get("template_data", {})
        from_email = inputs.get("from", self.credentials.get("from_email", ""))

        if not to_email or not template_id:
            return ConnectorResult(
                success=False, error="To and template_id are required"
            )

        # Currently only supports SendGrid templates
        if self.credentials.get("sendgrid_key"):
            api_key = self.credentials.get("sendgrid_key", "")

            payload = {
                "personalizations": [
                    {
                        "to": [{"email": to_email}],
                        "dynamic_template_data": template_data,
                    }
                ],
                "from": {"email": from_email},
                "template_id": template_id,
            }

            return await self._request(
                "POST",
                "https://api.sendgrid.com/v3/mail/send",
                headers={"Authorization": f"Bearer {api_key}"},
                json=payload,
            )

        return ConnectorResult(
            success=False, error="Template emails require SendGrid credentials"
        )

    async def test_connection(self) -> ConnectorResult:
        """Test the email connection."""
        if self.credentials.get("sendgrid_key"):
            # Verify SendGrid API key
            return await self._request(
                "GET",
                "https://api.sendgrid.com/v3/user/profile",
                headers={"Authorization": f"Bearer {self.credentials['sendgrid_key']}"},
            )
        elif self.credentials.get("smtp_host"):
            # Test SMTP connection
            try:
                smtp_host = self.credentials.get("smtp_host", "")
                smtp_port = int(self.credentials.get("smtp_port", 587))
                smtp_user = self.credentials.get("smtp_user", "")
                smtp_pass = self.credentials.get("smtp_pass", "")

                server = smtplib.SMTP(smtp_host, smtp_port)
                server.starttls()
                server.login(smtp_user, smtp_pass)
                server.quit()

                return ConnectorResult(
                    success=True, data={"message": "SMTP connection successful"}
                )
            except Exception as e:
                return ConnectorResult(success=False, error=str(e))

        return ConnectorResult(success=False, error="No email credentials configured")
