"""
Feedback Signal Capture - Brown Belt Lab 3, Story 3.2

Captures positive and negative feedback signals for model retraining:
- Positive: estimate_accepted, accurate_estimate, efficient_scheduling
- Negative: estimate_rejected, inaccurate_estimate, scheduling_conflict
"""

from datetime import datetime
from typing import Any, Callable, Dict, Optional
import boto3
import json
import functools


class FeedbackSignalCapture:
    """
    Capture positive and negative feedback signals.

    Example usage:
        capture = FeedbackSignalCapture(data_lake_bucket="zevbit-data-flywheel-123456")

        @capture.capture_signal("estimate_accepted")
        def on_customer_approval(estimate):
            return {"estimated_cost": estimate.total, "actual_approval": True}

        # When customer approves, signal is automatically captured
        on_customer_approval(estimate)
    """

    def __init__(self, data_lake_bucket: str, region: str = "us-east-1"):
        """
        Initialize feedback signal capture.

        Args:
            data_lake_bucket: S3 bucket for data lake
            region: AWS region
        """
        self.bucket = data_lake_bucket
        self.region = region
        self.s3 = boto3.client('s3', region_name=region)

    def capture_signal(self, signal_type: str):
        """
        Decorator to capture feedback signals.

        Args:
            signal_type: Type of signal (estimate_accepted, estimate_rejected, etc.)

        Returns:
            Wrapped function that captures and sends signals
        """
        def decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                # Execute function
                result = func(*args, **kwargs)

                # Capture feedback signal
                signal = {
                    "signal_type": signal_type,
                    "timestamp": datetime.utcnow().isoformat(),
                    "workflow_id": self._extract_workflow_id(args, kwargs),
                    "project_id": self._extract_project_id(args, kwargs),
                    "model_version": self._get_model_version(),
                    "data": result
                }

                # Redact PII before ingestion
                clean_signal = self._redact_pii(signal)

                # Send to data lake
                self._send_to_data_lake(clean_signal)

                # Check for retraining triggers
                self._check_retraining_triggers(signal_type, clean_signal)

                return result
            return wrapper
        return decorator

    def capture_manual_signal(self, signal_type: str, data: Dict):
        """
        Manually capture a signal (not via decorator).

        Args:
            signal_type: Type of signal
            data: Signal data
        """
        signal = {
            "signal_type": signal_type,
            "timestamp": datetime.utcnow().isoformat(),
            "workflow_id": data.get("workflow_id", "unknown"),
            "project_id": data.get("project_id", "unknown"),
            "model_version": self._get_model_version(),
            "data": data
        }

        clean_signal = self._redact_pii(signal)
        self._send_to_data_lake(clean_signal)
        self._check_retraining_triggers(signal_type, clean_signal)

    def _extract_workflow_id(self, args: tuple, kwargs: dict) -> str:
        """Extract workflow ID from args/kwargs"""
        # Try to find workflow_id in kwargs
        if "workflow_id" in kwargs:
            return kwargs["workflow_id"]

        # Try to find in first arg if it's a dict
        if args and isinstance(args[0], dict) and "workflow_id" in args[0]:
            return args[0]["workflow_id"]

        return "unknown"

    def _extract_project_id(self, args: tuple, kwargs: dict) -> str:
        """Extract project ID from args/kwargs"""
        if "project_id" in kwargs:
            return kwargs["project_id"]

        if args and isinstance(args[0], dict) and "project_id" in args[0]:
            return args[0]["project_id"]

        return "unknown"

    def _get_model_version(self) -> str:
        """Get current model version"""
        # TODO (Story 3.2): Get from environment or config
        return "claude-3-5-sonnet-20241022-v2:0"

    def _redact_pii(self, signal: Dict) -> Dict:
        """
        Redact PII from signal before sending to data lake.

        TODO (Story 3.2): Integrate with privacy/pii_redactor.py
        """
        # Import here to avoid circular dependency
        try:
            from zevbit_data_flywheel.privacy import PIIRedactor
            redactor = PIIRedactor()
            return redactor.redact(signal)
        except ImportError:
            # Fallback: basic PII redaction
            import re
            signal_str = json.dumps(signal)
            # Redact emails
            signal_str = re.sub(r'\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b', '[REDACTED_EMAIL]', signal_str)
            # Redact phones
            signal_str = re.sub(r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b', '[REDACTED_PHONE]', signal_str)
            return json.loads(signal_str)

    def _send_to_data_lake(self, signal: Dict):
        """
        Send signal to S3 data lake.

        Data is partitioned by:
        - signal_type (estimate_accepted, estimate_rejected, etc.)
        - date (YYYY/MM/DD)

        TODO (Story 3.2): Students implement robust S3 ingestion
        - Error handling and retries
        - Batch uploads for efficiency
        - Compression (gzip)
        """
        # Partition by date and signal type
        date = datetime.utcnow().strftime("%Y/%m/%d")
        signal_type = signal['signal_type']
        timestamp = signal['timestamp']

        key = f"signals/{signal_type}/{date}/{timestamp}.json"

        try:
            # Upload to S3 with encryption
            self.s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=json.dumps(signal, indent=2),
                ServerSideEncryption='AES256',  # Encrypt at rest
                ContentType='application/json',
                Metadata={
                    'signal_type': signal_type,
                    'workflow_id': signal.get('workflow_id', 'unknown')
                }
            )
            print(f"✅ Signal sent to data lake: s3://{self.bucket}/{key}")

        except Exception as e:
            print(f"❌ Failed to send signal to data lake: {e}")
            # TODO (Story 3.2): Implement retry logic or dead letter queue

    def _check_retraining_triggers(self, signal_type: str, signal: Dict):
        """
        Check if signal should trigger model retraining.

        Retraining triggers:
        - ≥10 similar patterns (e.g., 10 clay soil projects over estimate)
        - Accuracy drift (MAPE increases from 6% to 10%)
        - Low confidence patterns (≥20% decisions with confidence <0.7)

        TODO (Story 3.2): Implement pattern detection
        - Query recent signals from data lake
        - Detect patterns using analytics
        - Trigger retraining via SNS/SQS
        """
        # Simple example: Flag negative signals
        negative_signals = [
            "estimate_rejected",
            "inaccurate_estimate",
            "scheduling_conflict",
            "low_satisfaction"
        ]

        if signal_type in negative_signals:
            print(f"⚠️  Negative signal detected: {signal_type}")
            # TODO: Aggregate and check if threshold reached for retraining


# Signal type definitions for reference
class SignalTypes:
    """Predefined signal types"""

    # Positive signals
    ESTIMATE_ACCEPTED = "estimate_accepted"
    ACCURATE_ESTIMATE = "accurate_estimate"
    EFFICIENT_SCHEDULING = "efficient_scheduling"
    HIGH_SATISFACTION = "high_satisfaction"

    # Negative signals
    ESTIMATE_REJECTED = "estimate_rejected"
    INACCURATE_ESTIMATE = "inaccurate_estimate"
    SCHEDULING_CONFLICT = "scheduling_conflict"
    LOW_SATISFACTION = "low_satisfaction"

    # Variance signals
    COST_OVERRUN = "cost_overrun"
    COST_UNDERRUN = "cost_underrun"
    SCHEDULE_DELAY = "schedule_delay"


# Example usage
if __name__ == "__main__":
    # Initialize (you'll need a real S3 bucket for this to work)
    capture = FeedbackSignalCapture(
        data_lake_bucket="zevbit-data-flywheel-example"
    )

    # Example 1: Capture estimate acceptance
    @capture.capture_signal(SignalTypes.ESTIMATE_ACCEPTED)
    def on_estimate_accepted(workflow_id: str, estimated_cost: float):
        return {
            "workflow_id": workflow_id,
            "estimated_cost": estimated_cost,
            "approval_timestamp": datetime.utcnow().isoformat()
        }

    # Example 2: Capture variance (cost overrun)
    @capture.capture_signal(SignalTypes.COST_OVERRUN)
    def on_project_completion(workflow_id: str, estimated: float, actual: float):
        variance = ((actual - estimated) / estimated) * 100
        return {
            "workflow_id": workflow_id,
            "estimated_cost": estimated,
            "actual_cost": actual,
            "variance_percent": variance,
            "variance_category": "overrun" if variance > 0 else "underrun"
        }

    # Simulate signal capture
    print("📊 Simulating feedback signal capture...\n")

    # Note: This will fail without a real S3 bucket configured
    # on_estimate_accepted("workflow_123", 8500.00)
    # on_project_completion("workflow_124", 8400.00, 10500.00)  # 25% overrun

    print("\n✅ Feedback signal capture module ready!")
    print("   Configure S3 bucket and deploy to capture real signals.")
