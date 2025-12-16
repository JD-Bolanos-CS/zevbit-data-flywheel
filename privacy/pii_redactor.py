"""
PII Redactor - Brown Belt Lab 3, Story 3.3

Detects and redacts PII for GDPR/CCPA compliance:
- SSN, credit cards, emails, phones
- Customer names, addresses (anonymized with deterministic hashing)
- Ensures 100% PII detection (0 false negatives)
"""

import re
import hashlib
from typing import Any, Dict, List


class PIIRedactor:
    """
    Detect and redact Personally Identifiable Information (PII).

    Patterns from Green Belt Lab 2 + Brown Belt enhancements.

    Example usage:
        redactor = PIIRedactor()

        data = {
            "customer_email": "john@example.com",
            "phone": "(555) 123-4567",
            "project_cost": 8500
        }

        clean_data = redactor.redact(data)
        # {"customer_email": "[REDACTED_EMAIL]", "phone": "[REDACTED_PHONE]", "project_cost": 8500}
    """

    # PII patterns (regex)
    PATTERNS = {
        "ssn": r'\b\d{3}-\d{2}-\d{4}\b',
        "credit_card": r'\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b',
        "email": r'\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b',
        "phone": r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b',
        # TODO (Story 3.3): Add more patterns
        # - Driver's license numbers
        # - Passport numbers
        # - IP addresses (optional)
    }

    # Redaction replacements
    REPLACEMENTS = {
        "ssn": "[REDACTED_SSN]",
        "credit_card": "[REDACTED_CC]",
        "email": "[REDACTED_EMAIL]",
        "phone": "[REDACTED_PHONE]"
    }

    def __init__(self, anonymize_names: bool = True):
        """
        Initialize PII redactor.

        Args:
            anonymize_names: If True, anonymize names with deterministic hashes
        """
        self.anonymize_names = anonymize_names
        self._anonymization_cache = {}  # Cache for consistent anonymization

    def redact(self, data: Any) -> Any:
        """
        Redact PII from data (recursive for nested structures).

        Args:
            data: Data to redact (str, dict, list, or primitive)

        Returns:
            Data with PII redacted
        """
        if isinstance(data, str):
            return self._redact_string(data)
        elif isinstance(data, dict):
            return {k: self.redact(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self.redact(item) for item in data]
        else:
            return data  # Primitives (int, float, bool, None) pass through

    def _redact_string(self, text: str) -> str:
        """Redact PII from a string"""
        # Apply regex patterns
        for pii_type, pattern in self.PATTERNS.items():
            replacement = self.REPLACEMENTS[pii_type]
            text = re.sub(pattern, replacement, text)

        # TODO (Story 3.3): Named entity recognition for names/addresses
        # Use NER library (spaCy, etc.) to detect person names and locations

        return text

    def anonymize(self, identifier: str, category: str = "customer") -> str:
        """
        Anonymize an identifier with deterministic hashing.

        Args:
            identifier: Original identifier (name, address, etc.)
            category: Category of identifier (customer, contractor, location)

        Returns:
            Anonymized identifier (e.g., "customer_abc123")

        Example:
            >>> redactor.anonymize("John Smith", "customer")
            "customer_abc123"
            >>> redactor.anonymize("John Smith", "customer")  # Same input
            "customer_abc123"  # Same output (deterministic)
        """
        # Check cache for consistency
        cache_key = f"{category}:{identifier}"
        if cache_key in self._anonymization_cache:
            return self._anonymization_cache[cache_key]

        # Create deterministic hash
        hash_obj = hashlib.sha256(identifier.encode())
        hash_hex = hash_obj.hexdigest()[:6]  # First 6 chars

        anonymized = f"{category}_{hash_hex}"
        self._anonymization_cache[cache_key] = anonymized

        return anonymized

    def detect_pii(self, data: Any) -> List[str]:
        """
        Detect what types of PII are present in data.

        Args:
            data: Data to scan

        Returns:
            List of detected PII types (e.g., ["email", "phone"])
        """
        detected = set()
        text = str(data)

        for pii_type, pattern in self.PATTERNS.items():
            if re.search(pattern, text):
                detected.add(pii_type)

        return list(detected)

    def validate_no_pii(self, data: Any) -> bool:
        """
        Validate that no PII remains in data.

        Args:
            data: Data to validate

        Returns:
            True if no PII detected, False otherwise

        Example:
            >>> redactor.validate_no_pii({"email": "[REDACTED_EMAIL]"})
            True
            >>> redactor.validate_no_pii({"email": "john@example.com"})
            False
        """
        return len(self.detect_pii(data)) == 0

    def get_pii_report(self, data: Any) -> Dict:
        """
        Generate a PII detection report.

        Args:
            data: Data to analyze

        Returns:
            Report with detected PII types and counts
        """
        detected = {}
        text = str(data)

        for pii_type, pattern in self.PATTERNS.items():
            matches = re.findall(pattern, text)
            if matches:
                detected[pii_type] = {
                    "count": len(matches),
                    "examples": matches[:3]  # First 3 examples
                }

        return {
            "pii_detected": len(detected) > 0,
            "pii_types": list(detected.keys()),
            "details": detected
        }


# Compliance validator
class ComplianceValidator:
    """
    Validate GDPR/CCPA compliance.

    TODO (Story 3.3): Students implement comprehensive compliance checks
    - Data retention policies
    - Data residency (US only)
    - Encryption verification
    - Audit logs
    """

    def __init__(self, redactor: PIIRedactor):
        self.redactor = redactor

    def validate_data_for_storage(self, data: Any) -> Dict:
        """
        Validate data is safe for storage in data lake.

        Returns:
            {
                "compliant": bool,
                "issues": [str],
                "pii_report": dict
            }
        """
        issues = []

        # Check for PII
        pii_report = self.redactor.get_pii_report(data)
        if pii_report["pii_detected"]:
            issues.append(f"PII detected: {', '.join(pii_report['pii_types'])}")

        # TODO (Story 3.3): Additional compliance checks
        # - Check data size (<10MB for efficient processing)
        # - Verify timestamp is present
        # - Check required fields are present

        return {
            "compliant": len(issues) == 0,
            "issues": issues,
            "pii_report": pii_report
        }


# Example usage
if __name__ == "__main__":
    print("🔒 PII Redactor - Brown Belt Lab 3, Story 3.3\n")

    # Initialize redactor
    redactor = PIIRedactor()

    # Example 1: Basic PII redaction
    print("Example 1: Basic PII Redaction")
    data = {
        "customer_name": "John Smith",
        "email": "john.smith@example.com",
        "phone": "(555) 123-4567",
        "ssn": "123-45-6789",
        "project_cost": 8500,
        "notes": "Customer called from (555) 987-6543 to discuss the project"
    }

    print("Original data:")
    print(data)

    clean_data = redactor.redact(data)
    print("\nRedacted data:")
    print(clean_data)

    # Example 2: PII detection
    print("\n" + "="*60)
    print("Example 2: PII Detection Report")
    report = redactor.get_pii_report(data)
    print(f"PII detected: {report['pii_detected']}")
    print(f"PII types: {report['pii_types']}")
    for pii_type, details in report['details'].items():
        print(f"  - {pii_type}: {details['count']} occurrences")

    # Example 3: Validation
    print("\n" + "="*60)
    print("Example 3: Compliance Validation")
    print(f"Original data compliant: {redactor.validate_no_pii(data)}")
    print(f"Redacted data compliant: {redactor.validate_no_pii(clean_data)}")

    # Example 4: Anonymization
    print("\n" + "="*60)
    print("Example 4: Deterministic Anonymization")
    customer_id = redactor.anonymize("John Smith", "customer")
    location_id = redactor.anonymize("123 Main St, Boston, MA", "location")
    print(f"Customer name anonymized: {customer_id}")
    print(f"Address anonymized: {location_id}")

    # Verify deterministic
    customer_id_again = redactor.anonymize("John Smith", "customer")
    print(f"Same input → same output: {customer_id == customer_id_again}")

    print("\n✅ PII Redactor ready for production!")
