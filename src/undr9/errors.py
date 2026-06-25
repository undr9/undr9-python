class Undr9Error(Exception):
    """Base SDK error."""


class Undr9ApiError(Undr9Error):
    """Represents a structured error returned by the UNDR9 HTTP API."""

    def __init__(self, status_code: int, code: str, message: str, details: list[str] | None = None):
        super().__init__(f"{code}: {message}")
        self.status_code = status_code
        self.code = code
        self.message = message
        self.details = details or []


class Undr9ConnectionError(Undr9Error):
    """Raised when the SDK cannot connect to the UNDR9 server."""

    def __init__(self, base_url: str, reason: str):
        super().__init__(f"unable to connect to UNDR9 at {base_url}: {reason}")
        self.base_url = base_url
        self.reason = reason
