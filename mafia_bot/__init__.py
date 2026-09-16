"""Public handler package.

Stage 1 keeps runtime compatibility while exposing handlers by domain.
"""

from .._handlers_legacy import router, restore_runtime_state

__all__ = ["router", "restore_runtime_state"]
