"""
Provenance Tracker - Brown Belt Lab 3, Story 3.1

Tracks provenance for all agent decisions:
- Decision: What the agent decided
- Reasoning: Why the agent decided
- Data Sources: What data informed the decision
- Model Version: Which model/tool was used
- Timestamp: When the decision was made
- Confidence: Model confidence score (0-1)
"""

from datetime import datetime
from typing import Any, Callable, Dict, Optional
import json
import functools


class ProvenanceTracker:
    """
    Track provenance for all agent decisions.

    Example usage:
        tracker = ProvenanceTracker()

        @tracker.track_decision
        def estimate_cost(sqft: int) -> dict:
            return {
                "total_cost": sqft * 1.5,
                "reasoning": "Based on $1.50/sqft for sod installation",
                "confidence": 0.87
            }

        result = estimate_cost(5000)
        # Provenance automatically captured and stored
    """

    def __init__(self, storage_backend: Optional[str] = "memory"):
        """
        Initialize provenance tracker.

        Args:
            storage_backend: Where to store provenance (memory, dynamodb, s3)
        """
        self.storage_backend = storage_backend
        self.provenance_store = []  # In-memory storage for demo

    def track_decision(self, func: Callable) -> Callable:
        """
        Decorator to track agent decisions with provenance.

        Args:
            func: Agent decision function to track

        Returns:
            Wrapped function that captures provenance
        """
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Execute agent decision
            result = func(*args, **kwargs)

            # Capture provenance
            provenance = {
                "decision": self._format_decision(func.__name__, result),
                "reasoning": self._extract_reasoning(result),
                "data_sources": self._identify_data_sources(func, args, kwargs),
                "model_version": self._get_model_version(),
                "timestamp": datetime.utcnow().isoformat(),
                "confidence": self._extract_confidence(result),
                "function": func.__name__,
                "module": func.__module__
            }

            # Store provenance (queryable)
            self._store_provenance(provenance)

            # Flag low confidence
            if provenance["confidence"] < 0.7:
                self._flag_for_review(provenance)

            return result
        return wrapper

    def _format_decision(self, func_name: str, result: Any) -> str:
        """Format the decision in human-readable form"""
        if isinstance(result, dict):
            # Extract key decision points
            summary = {k: v for k, v in result.items() if k in ["total_cost", "status", "approved", "recommended"]}
            return f"{func_name} decided: {summary}"
        return f"{func_name} returned: {str(result)[:100]}"

    def _extract_reasoning(self, result: Any) -> str:
        """Extract reasoning from agent result"""
        if isinstance(result, dict) and "reasoning" in result:
            return result["reasoning"]
        return "No explicit reasoning provided"

    def _identify_data_sources(self, func: Callable, args: tuple, kwargs: dict) -> Dict:
        """
        Identify what data informed the decision.

        TODO (Story 3.1): Students implement comprehensive data source tracking
        - Track which MCP tools were called
        - Track which external APIs were used
        - Track which database queries were run
        """
        return {
            "function_signature": f"{func.__name__}({', '.join([str(a)[:50] for a in args])})",
            "kwargs": {k: str(v)[:50] for k, v in kwargs.items()},
            # TODO: Add actual data source tracking here
        }

    def _get_model_version(self) -> str:
        """Get current model version"""
        # TODO (Story 3.1): Get actual model version from environment or config
        return "claude-3-5-sonnet-20241022-v2:0"

    def _extract_confidence(self, result: Any) -> float:
        """Extract confidence score if available"""
        if isinstance(result, dict) and "confidence" in result:
            return result["confidence"]
        # TODO (Story 3.1): Implement confidence estimation for results without explicit confidence
        return 0.8  # Default confidence

    def _store_provenance(self, provenance: Dict):
        """
        Store provenance for querying.

        TODO (Story 3.1): Students implement persistent storage
        - Option 1: DynamoDB for queryable provenance
        - Option 2: S3 with partitioning for cost-effective storage
        - Option 3: Both (DynamoDB for recent, S3 for archive)
        """
        self.provenance_store.append(provenance)
        print(f"📋 Provenance stored: {provenance['function']} (confidence: {provenance['confidence']:.2f})")

    def _flag_for_review(self, provenance: Dict):
        """Flag low confidence decisions for human review"""
        print(f"⚠️  Low confidence decision flagged for review:")
        print(f"   Function: {provenance['function']}")
        print(f"   Confidence: {provenance['confidence']:.2f}")
        print(f"   Decision: {provenance['decision']}")

        # TODO (Story 3.1): Send to review queue (SQS, SNS, etc.)

    def query(self, workflow_id: Optional[str] = None,
              decision_type: Optional[str] = None,
              start_time: Optional[str] = None,
              end_time: Optional[str] = None) -> list[Dict]:
        """
        Query provenance data.

        Args:
            workflow_id: Filter by workflow ID
            decision_type: Filter by decision type (function name)
            start_time: Filter by start timestamp
            end_time: Filter by end timestamp

        Returns:
            List of matching provenance records

        TODO (Story 3.1): Implement efficient querying
        - If using DynamoDB: Use query with GSI
        - If using S3: Use Athena or partition pruning
        """
        results = self.provenance_store

        if decision_type:
            results = [p for p in results if p['function'] == decision_type]

        if start_time:
            results = [p for p in results if p['timestamp'] >= start_time]

        if end_time:
            results = [p for p in results if p['timestamp'] <= end_time]

        return results


# Example usage
if __name__ == "__main__":
    # Initialize tracker
    tracker = ProvenanceTracker()

    # Example: Track a cost estimation decision
    @tracker.track_decision
    def estimate_project_cost(sqft: int, project_type: str) -> dict:
        """Estimate project cost"""
        cost_per_sqft = {"lawn": 1.50, "garden": 2.00}.get(project_type, 1.75)
        total = sqft * cost_per_sqft

        return {
            "total_cost": total,
            "cost_per_sqft": cost_per_sqft,
            "reasoning": f"Based on ${cost_per_sqft}/sqft for {project_type} projects using historical data",
            "confidence": 0.87
        }

    # Make a decision (provenance automatically tracked)
    result = estimate_project_cost(5000, "lawn")
    print(f"\nResult: ${result['total_cost']:,.2f}")

    # Query provenance
    print("\n📊 Provenance Query Results:")
    all_provenance = tracker.query()
    for p in all_provenance:
        print(json.dumps(p, indent=2))
