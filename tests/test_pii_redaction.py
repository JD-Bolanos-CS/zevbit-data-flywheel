"""
Test PII Redaction - Brown Belt Lab 3, Story 3.3

Tests for PII detection and redaction to ensure GDPR/CCPA compliance.
Success criteria: 100% PII detection (0 false negatives)
"""

import pytest
from privacy.pii_redactor import PIIRedactor, ComplianceValidator


class TestPIIRedaction:
    """Test suite for PII redaction"""

    def setup_method(self):
        """Set up test fixtures"""
        self.redactor = PIIRedactor()

    def test_email_redaction(self):
        """Test email address redaction"""
        data = {"customer_email": "john@example.com"}
        clean = self.redactor.redact(data)

        assert clean["customer_email"] == "[REDACTED_EMAIL]"
        assert self.redactor.validate_no_pii(clean)

    def test_phone_redaction(self):
        """Test phone number redaction (multiple formats)"""
        test_cases = [
            "(555) 123-4567",
            "555-123-4567",
            "555.123.4567",
            "5551234567"
        ]

        for phone in test_cases:
            data = {"phone": phone}
            clean = self.redactor.redact(data)
            assert "[REDACTED_PHONE]" in str(clean)

    def test_ssn_redaction(self):
        """Test SSN redaction"""
        data = {"ssn": "123-45-6789"}
        clean = self.redactor.redact(data)

        assert clean["ssn"] == "[REDACTED_SSN]"
        assert self.redactor.validate_no_pii(clean)

    def test_credit_card_redaction(self):
        """Test credit card redaction"""
        test_cards = [
            "1234-5678-9012-3456",
            "1234 5678 9012 3456",
            "1234567890123456"
        ]

        for card in test_cards:
            data = {"card": card}
            clean = self.redactor.redact(data)
            assert "[REDACTED_CC]" in str(clean)

    def test_nested_structure_redaction(self):
        """Test PII redaction in nested structures"""
        data = {
            "customer": {
                "name": "John Smith",
                "contact": {
                    "email": "john@example.com",
                    "phone": "(555) 123-4567"
                },
                "projects": [
                    {"id": 1, "contact_email": "project1@example.com"},
                    {"id": 2, "contact_email": "project2@example.com"}
                ]
            },
            "cost": 8500  # Non-PII should remain
        }

        clean = self.redactor.redact(data)

        # Verify PII redacted
        assert "[REDACTED_EMAIL]" in str(clean)
        assert "[REDACTED_PHONE]" in str(clean)

        # Verify non-PII preserved
        assert clean["cost"] == 8500
        assert clean["customer"]["projects"][0]["id"] == 1

    def test_pii_detection(self):
        """Test PII detection without redaction"""
        data = {
            "email": "test@example.com",
            "phone": "(555) 123-4567",
            "cost": 5000
        }

        detected = self.redactor.detect_pii(data)

        assert "email" in detected
        assert "phone" in detected
        assert len(detected) == 2  # Only email and phone

    def test_false_negatives(self):
        """
        CRITICAL TEST: Ensure 0 false negatives.
        All PII must be detected (100% recall).
        """
        # Comprehensive PII test cases
        pii_test_cases = [
            ("email", "user@domain.com"),
            ("email", "first.last@company.co.uk"),
            ("phone", "(555) 123-4567"),
            ("phone", "555-123-4567"),
            ("ssn", "123-45-6789"),
            ("credit_card", "1234-5678-9012-3456"),
            ("credit_card", "1234567890123456")
        ]

        false_negatives = []

        for pii_type, pii_value in pii_test_cases:
            detected = self.redactor.detect_pii(pii_value)
            if pii_type not in detected:
                false_negatives.append((pii_type, pii_value))

        # MUST be zero false negatives
        assert len(false_negatives) == 0, f"False negatives detected: {false_negatives}"

    def test_anonymization_deterministic(self):
        """Test that anonymization is deterministic"""
        name = "John Smith"

        id1 = self.redactor.anonymize(name, "customer")
        id2 = self.redactor.anonymize(name, "customer")

        # Same input must produce same output
        assert id1 == id2

    def test_anonymization_different_categories(self):
        """Test that different categories produce different anonymizations"""
        identifier = "John Smith"

        customer_id = self.redactor.anonymize(identifier, "customer")
        contractor_id = self.redactor.anonymize(identifier, "contractor")

        # Different categories should produce different IDs
        assert customer_id != contractor_id
        assert "customer" in customer_id
        assert "contractor" in contractor_id

    def test_compliance_validation(self):
        """Test compliance validator"""
        validator = ComplianceValidator(self.redactor)

        # Test with PII (should fail)
        data_with_pii = {"email": "john@example.com"}
        result = validator.validate_data_for_storage(data_with_pii)

        assert result["compliant"] == False
        assert len(result["issues"]) > 0
        assert result["pii_report"]["pii_detected"] == True

        # Test without PII (should pass)
        clean_data = self.redactor.redact(data_with_pii)
        result = validator.validate_data_for_storage(clean_data)

        assert result["compliant"] == True
        assert len(result["issues"]) == 0

    def test_pii_report(self):
        """Test PII detection report"""
        data = {
            "customer_email": "john@example.com",
            "backup_email": "backup@example.com",
            "phone": "(555) 123-4567",
            "cost": 8500
        }

        report = self.redactor.get_pii_report(data)

        assert report["pii_detected"] == True
        assert "email" in report["pii_types"]
        assert "phone" in report["pii_types"]
        assert report["details"]["email"]["count"] == 2  # Two emails
        assert report["details"]["phone"]["count"] == 1


# Run tests
if __name__ == "__main__":
    pytest.main([__file__, "-v"])
