from __future__ import annotations

import asyncio
import json
import time
import urllib.error
import urllib.request
from collections.abc import AsyncIterator, Iterator
from collections.abc import Iterable, Mapping
from typing import Any

from .errors import Undr9ApiError, Undr9ConnectionError

try:
    import httpx
except ImportError:  # pragma: no cover - exercised only when optional dependency is missing
    httpx = None


DEFAULT_RETRY_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})
SAFE_RETRY_METHODS = frozenset({"DELETE", "GET", "HEAD", "OPTIONS"})
SAFE_RETRY_POST_PATHS = frozenset({"/v1/query", "/v1/query/stream"})


def _normalize_retry_status_codes(
    retry_status_codes: Iterable[int] | None,
) -> frozenset[int]:
    if retry_status_codes is None:
        return DEFAULT_RETRY_STATUS_CODES
    return frozenset(retry_status_codes)


def _request_is_retryable(
    method: str,
    path: str,
    *,
    retry_non_idempotent_requests: bool,
) -> bool:
    method = method.upper()
    if retry_non_idempotent_requests:
        return True
    return method in SAFE_RETRY_METHODS or (method == "POST" and path in SAFE_RETRY_POST_PATHS)


def _retry_after_seconds(headers: Mapping[str, str] | None) -> float | None:
    if not headers:
        return None
    retry_after = headers.get("retry-after") or headers.get("Retry-After")
    if retry_after is None:
        return None
    try:
        return max(float(retry_after), 0.0)
    except ValueError:
        return None


def _retry_delay_seconds(
    retry_index: int,
    *,
    retry_backoff_seconds: float,
    retry_after_seconds: float | None,
) -> float:
    if retry_after_seconds is not None:
        return retry_after_seconds
    return max(retry_backoff_seconds, 0.0) * (2**retry_index)


def _parse_error_payload(body: str, *, fallback_message: str) -> tuple[str, str, list[str]]:
    try:
        payload = json.loads(body) if body else {}
    except json.JSONDecodeError:
        payload = {}
    return (
        payload.get("code", "http_error"),
        payload.get("message", fallback_message),
        payload.get("details", []),
    )


def _parse_ndjson_line(raw_line: bytes | str) -> dict[str, Any] | None:
    if isinstance(raw_line, bytes):
        line = raw_line.decode("utf-8").strip()
    else:
        line = raw_line.strip()
    if not line:
        return None
    return json.loads(line)


def _build_headers(
    api_key: str,
    *,
    headers: Mapping[str, str] | None,
    user_agent: str | None,
) -> dict[str, str]:
    resolved_headers = dict(headers or {})
    if user_agent is not None:
        resolved_headers["user-agent"] = user_agent
    resolved_headers["x-api-key"] = api_key
    return resolved_headers


def _build_httpx_timeout(
    timeout: float | None,
    *,
    connect_timeout: float | None,
    read_timeout: float | None,
    write_timeout: float | None,
    pool_timeout: float | None,
) -> Any:
    if httpx is None:
        return timeout
    if all(
        value is None
        for value in (connect_timeout, read_timeout, write_timeout, pool_timeout)
    ):
        return timeout
    return httpx.Timeout(
        timeout,
        connect=connect_timeout if connect_timeout is not None else timeout,
        read=read_timeout if read_timeout is not None else timeout,
        write=write_timeout if write_timeout is not None else timeout,
        pool=pool_timeout if pool_timeout is not None else timeout,
    )


def _build_httpx_limits(
    *,
    max_connections: int | None,
    max_keepalive_connections: int | None,
    keepalive_expiry: float | None,
) -> Any:
    if httpx is None:
        return None
    if (
        max_connections is None
        and max_keepalive_connections is None
        and keepalive_expiry is None
    ):
        return None
    return httpx.Limits(
        max_connections=max_connections,
        max_keepalive_connections=max_keepalive_connections,
        keepalive_expiry=keepalive_expiry,
    )


class HttpTransport:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        timeout: float | None = 10.0,
        max_retries: int = 0,
        retry_backoff_seconds: float = 0.25,
        retry_non_idempotent_requests: bool = False,
        retry_status_codes: Iterable[int] | None = None,
        headers: Mapping[str, str] | None = None,
        user_agent: str | None = None,
        http2: bool = False,
        follow_redirects: bool = False,
        verify: bool | str = True,
        max_connections: int | None = None,
        max_keepalive_connections: int | None = None,
        keepalive_expiry: float | None = None,
        connect_timeout: float | None = None,
        read_timeout: float | None = None,
        write_timeout: float | None = None,
        pool_timeout: float | None = None,
    ):
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._timeout = timeout
        self._headers = _build_headers(
            api_key,
            headers=headers,
            user_agent=user_agent,
        )
        self._max_retries = max(max_retries, 0)
        self._retry_backoff_seconds = retry_backoff_seconds
        self._retry_non_idempotent_requests = retry_non_idempotent_requests
        self._retry_status_codes = _normalize_retry_status_codes(retry_status_codes)
        self._client = None
        if httpx is not None:
            client_kwargs = {
                "base_url": self._base_url,
                "headers": self._headers,
                "timeout": _build_httpx_timeout(
                    self._timeout,
                    connect_timeout=connect_timeout,
                    read_timeout=read_timeout,
                    write_timeout=write_timeout,
                    pool_timeout=pool_timeout,
                ),
                "http2": http2,
                "follow_redirects": follow_redirects,
                "verify": verify,
            }
            limits = _build_httpx_limits(
                max_connections=max_connections,
                max_keepalive_connections=max_keepalive_connections,
                keepalive_expiry=keepalive_expiry,
            )
            if limits is not None:
                client_kwargs["limits"] = limits
            self._client = httpx.Client(
                **client_kwargs,
            )

    def _urllib_request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> Any:
        url = f"{self._base_url}{path}"
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        headers = dict(self._headers)
        if body is not None:
            headers["content-type"] = "application/json"

        request = urllib.request.Request(url=url, data=body, headers=headers, method=method)
        retryable_request = _request_is_retryable(
            method,
            path,
            retry_non_idempotent_requests=self._retry_non_idempotent_requests,
        )

        for retry_index in range(self._max_retries + 1):
            try:
                with urllib.request.urlopen(request, timeout=self._timeout) as response:
                    data = response.read()
                    if not data:
                        return None
                    return json.loads(data.decode("utf-8"))
            except urllib.error.HTTPError as error:
                error_body = error.read().decode("utf-8")
                should_retry = (
                    retryable_request
                    and retry_index < self._max_retries
                    and error.code in self._retry_status_codes
                )
                if should_retry:
                    delay = _retry_delay_seconds(
                        retry_index,
                        retry_backoff_seconds=self._retry_backoff_seconds,
                        retry_after_seconds=_retry_after_seconds(error.headers),
                    )
                    if delay > 0:
                        time.sleep(delay)
                    continue

                code, message, details = _parse_error_payload(
                    error_body,
                    fallback_message=error.reason,
                )
                raise Undr9ApiError(
                    status_code=error.code,
                    code=code,
                    message=message,
                    details=details,
                ) from error
            except urllib.error.URLError as error:
                should_retry = retryable_request and retry_index < self._max_retries
                if should_retry:
                    delay = _retry_delay_seconds(
                        retry_index,
                        retry_backoff_seconds=self._retry_backoff_seconds,
                        retry_after_seconds=None,
                    )
                    if delay > 0:
                        time.sleep(delay)
                    continue

                raise Undr9ConnectionError(
                    base_url=self._base_url,
                    reason=str(error.reason),
                ) from error

    def _urllib_text_request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> str:
        url = f"{self._base_url}{path}"
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        headers = dict(self._headers)
        if body is not None:
            headers["content-type"] = "application/json"

        request = urllib.request.Request(url=url, data=body, headers=headers, method=method)
        retryable_request = _request_is_retryable(
            method,
            path,
            retry_non_idempotent_requests=self._retry_non_idempotent_requests,
        )

        for retry_index in range(self._max_retries + 1):
            try:
                with urllib.request.urlopen(request, timeout=self._timeout) as response:
                    return response.read().decode("utf-8")
            except urllib.error.HTTPError as error:
                error_body = error.read().decode("utf-8")
                should_retry = (
                    retryable_request
                    and retry_index < self._max_retries
                    and error.code in self._retry_status_codes
                )
                if should_retry:
                    delay = _retry_delay_seconds(
                        retry_index,
                        retry_backoff_seconds=self._retry_backoff_seconds,
                        retry_after_seconds=_retry_after_seconds(error.headers),
                    )
                    if delay > 0:
                        time.sleep(delay)
                    continue

                code, message, details = _parse_error_payload(
                    error_body,
                    fallback_message=error.reason,
                )
                raise Undr9ApiError(
                    status_code=error.code,
                    code=code,
                    message=message,
                    details=details,
                ) from error
            except urllib.error.URLError as error:
                should_retry = retryable_request and retry_index < self._max_retries
                if should_retry:
                    delay = _retry_delay_seconds(
                        retry_index,
                        retry_backoff_seconds=self._retry_backoff_seconds,
                        retry_after_seconds=None,
                    )
                    if delay > 0:
                        time.sleep(delay)
                    continue

                raise Undr9ConnectionError(
                    base_url=self._base_url,
                    reason=str(error.reason),
                ) from error

    def _urllib_stream_request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> Iterator[dict[str, Any]]:
        url = f"{self._base_url}{path}"
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        headers = dict(self._headers)
        if body is not None:
            headers["content-type"] = "application/json"

        request = urllib.request.Request(url=url, data=body, headers=headers, method=method)
        retryable_request = _request_is_retryable(
            method,
            path,
            retry_non_idempotent_requests=self._retry_non_idempotent_requests,
        )
        yielded_any = False

        for retry_index in range(self._max_retries + 1):
            try:
                with urllib.request.urlopen(request, timeout=self._timeout) as response:
                    for raw_line in response:
                        parsed = _parse_ndjson_line(raw_line)
                        if parsed is None:
                            continue
                        yielded_any = True
                        yield parsed
                    return
            except urllib.error.HTTPError as error:
                error_body = error.read().decode("utf-8")
                should_retry = (
                    not yielded_any
                    and retryable_request
                    and retry_index < self._max_retries
                    and error.code in self._retry_status_codes
                )
                if should_retry:
                    delay = _retry_delay_seconds(
                        retry_index,
                        retry_backoff_seconds=self._retry_backoff_seconds,
                        retry_after_seconds=_retry_after_seconds(error.headers),
                    )
                    if delay > 0:
                        time.sleep(delay)
                    continue

                code, message, details = _parse_error_payload(
                    error_body,
                    fallback_message=error.reason,
                )
                raise Undr9ApiError(
                    status_code=error.code,
                    code=code,
                    message=message,
                    details=details,
                ) from error
            except urllib.error.URLError as error:
                should_retry = (
                    not yielded_any and retryable_request and retry_index < self._max_retries
                )
                if should_retry:
                    delay = _retry_delay_seconds(
                        retry_index,
                        retry_backoff_seconds=self._retry_backoff_seconds,
                        retry_after_seconds=None,
                    )
                    if delay > 0:
                        time.sleep(delay)
                    continue

                raise Undr9ConnectionError(
                    base_url=self._base_url,
                    reason=str(error.reason),
                ) from error

    def request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> Any:
        if self._client is None:
            return self._urllib_request(method, path, payload)

        retryable_request = _request_is_retryable(
            method,
            path,
            retry_non_idempotent_requests=self._retry_non_idempotent_requests,
        )

        for retry_index in range(self._max_retries + 1):
            try:
                response = self._client.request(method=method, url=path, json=payload)
                response.raise_for_status()
            except httpx.HTTPStatusError as error:
                should_retry = (
                    retryable_request
                    and retry_index < self._max_retries
                    and error.response.status_code in self._retry_status_codes
                )
                if should_retry:
                    delay = _retry_delay_seconds(
                        retry_index,
                        retry_backoff_seconds=self._retry_backoff_seconds,
                        retry_after_seconds=_retry_after_seconds(error.response.headers),
                    )
                    if delay > 0:
                        time.sleep(delay)
                    continue

                code, message, details = _parse_error_payload(
                    error.response.text,
                    fallback_message=error.response.reason_phrase,
                )
                raise Undr9ApiError(
                    status_code=error.response.status_code,
                    code=code,
                    message=message,
                    details=details,
                ) from error
            except httpx.RequestError as error:
                should_retry = retryable_request and retry_index < self._max_retries
                if should_retry:
                    delay = _retry_delay_seconds(
                        retry_index,
                        retry_backoff_seconds=self._retry_backoff_seconds,
                        retry_after_seconds=None,
                    )
                    if delay > 0:
                        time.sleep(delay)
                    continue

                raise Undr9ConnectionError(
                    base_url=self._base_url,
                    reason=str(error),
                ) from error

            if not response.content:
                return None
            return response.json()

    def text_request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> str:
        if self._client is None:
            return self._urllib_text_request(method, path, payload)

        retryable_request = _request_is_retryable(
            method,
            path,
            retry_non_idempotent_requests=self._retry_non_idempotent_requests,
        )

        for retry_index in range(self._max_retries + 1):
            try:
                response = self._client.request(method=method, url=path, json=payload)
                response.raise_for_status()
                return response.text
            except httpx.HTTPStatusError as error:
                should_retry = (
                    retryable_request
                    and retry_index < self._max_retries
                    and error.response.status_code in self._retry_status_codes
                )
                if should_retry:
                    delay = _retry_delay_seconds(
                        retry_index,
                        retry_backoff_seconds=self._retry_backoff_seconds,
                        retry_after_seconds=_retry_after_seconds(error.response.headers),
                    )
                    if delay > 0:
                        time.sleep(delay)
                    continue
                code, message, details = _parse_error_payload(
                    error.response.text,
                    fallback_message=error.response.reason_phrase,
                )
                raise Undr9ApiError(
                    status_code=error.response.status_code,
                    code=code,
                    message=message,
                    details=details,
                ) from error
            except httpx.RequestError as error:
                should_retry = retryable_request and retry_index < self._max_retries
                if should_retry:
                    delay = _retry_delay_seconds(
                        retry_index,
                        retry_backoff_seconds=self._retry_backoff_seconds,
                        retry_after_seconds=None,
                    )
                    if delay > 0:
                        time.sleep(delay)
                    continue
                raise Undr9ConnectionError(
                    base_url=self._base_url,
                    reason=str(error),
                ) from error

    def stream_request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> Iterator[dict[str, Any]]:
        if self._client is None:
            yield from self._urllib_stream_request(method, path, payload)
            return

        retryable_request = _request_is_retryable(
            method,
            path,
            retry_non_idempotent_requests=self._retry_non_idempotent_requests,
        )
        yielded_any = False

        for retry_index in range(self._max_retries + 1):
            try:
                with self._client.stream(method=method, url=path, json=payload) as response:
                    response.raise_for_status()
                    for raw_line in response.iter_lines():
                        parsed = _parse_ndjson_line(raw_line)
                        if parsed is None:
                            continue
                        yielded_any = True
                        yield parsed
                    return
            except httpx.HTTPStatusError as error:
                should_retry = (
                    not yielded_any
                    and retryable_request
                    and retry_index < self._max_retries
                    and error.response.status_code in self._retry_status_codes
                )
                if should_retry:
                    delay = _retry_delay_seconds(
                        retry_index,
                        retry_backoff_seconds=self._retry_backoff_seconds,
                        retry_after_seconds=_retry_after_seconds(error.response.headers),
                    )
                    if delay > 0:
                        time.sleep(delay)
                    continue
                code, message, details = _parse_error_payload(
                    error.response.text,
                    fallback_message=error.response.reason_phrase,
                )
                raise Undr9ApiError(
                    status_code=error.response.status_code,
                    code=code,
                    message=message,
                    details=details,
                ) from error
            except httpx.RequestError as error:
                should_retry = (
                    not yielded_any and retryable_request and retry_index < self._max_retries
                )
                if should_retry:
                    delay = _retry_delay_seconds(
                        retry_index,
                        retry_backoff_seconds=self._retry_backoff_seconds,
                        retry_after_seconds=None,
                    )
                    if delay > 0:
                        time.sleep(delay)
                    continue
                raise Undr9ConnectionError(
                    base_url=self._base_url,
                    reason=str(error),
                ) from error

    def close(self) -> None:
        if self._client is not None:
            self._client.close()


class AsyncHttpTransport:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        timeout: float | None = 10.0,
        max_retries: int = 0,
        retry_backoff_seconds: float = 0.25,
        retry_non_idempotent_requests: bool = False,
        retry_status_codes: Iterable[int] | None = None,
        headers: Mapping[str, str] | None = None,
        user_agent: str | None = None,
        http2: bool = False,
        follow_redirects: bool = False,
        verify: bool | str = True,
        max_connections: int | None = None,
        max_keepalive_connections: int | None = None,
        keepalive_expiry: float | None = None,
        connect_timeout: float | None = None,
        read_timeout: float | None = None,
        write_timeout: float | None = None,
        pool_timeout: float | None = None,
    ):
        if httpx is None:
            raise ImportError(
                "AsyncUndr9Client requires the 'httpx' dependency. Install the SDK package or add httpx."
            )

        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._timeout = timeout
        self._headers = _build_headers(
            api_key,
            headers=headers,
            user_agent=user_agent,
        )
        self._max_retries = max(max_retries, 0)
        self._retry_backoff_seconds = retry_backoff_seconds
        self._retry_non_idempotent_requests = retry_non_idempotent_requests
        self._retry_status_codes = _normalize_retry_status_codes(retry_status_codes)
        client_kwargs = {
            "base_url": self._base_url,
            "headers": self._headers,
            "timeout": _build_httpx_timeout(
                self._timeout,
                connect_timeout=connect_timeout,
                read_timeout=read_timeout,
                write_timeout=write_timeout,
                pool_timeout=pool_timeout,
            ),
            "http2": http2,
            "follow_redirects": follow_redirects,
            "verify": verify,
        }
        limits = _build_httpx_limits(
            max_connections=max_connections,
            max_keepalive_connections=max_keepalive_connections,
            keepalive_expiry=keepalive_expiry,
        )
        if limits is not None:
            client_kwargs["limits"] = limits
        self._client = httpx.AsyncClient(**client_kwargs)

    async def request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> Any:
        retryable_request = _request_is_retryable(
            method,
            path,
            retry_non_idempotent_requests=self._retry_non_idempotent_requests,
        )

        for retry_index in range(self._max_retries + 1):
            try:
                response = await self._client.request(method=method, url=path, json=payload)
                response.raise_for_status()
            except httpx.HTTPStatusError as error:
                should_retry = (
                    retryable_request
                    and retry_index < self._max_retries
                    and error.response.status_code in self._retry_status_codes
                )
                if should_retry:
                    delay = _retry_delay_seconds(
                        retry_index,
                        retry_backoff_seconds=self._retry_backoff_seconds,
                        retry_after_seconds=_retry_after_seconds(error.response.headers),
                    )
                    if delay > 0:
                        await asyncio.sleep(delay)
                    continue

                code, message, details = _parse_error_payload(
                    error.response.text,
                    fallback_message=error.response.reason_phrase,
                )
                raise Undr9ApiError(
                    status_code=error.response.status_code,
                    code=code,
                    message=message,
                    details=details,
                ) from error
            except httpx.RequestError as error:
                should_retry = retryable_request and retry_index < self._max_retries
                if should_retry:
                    delay = _retry_delay_seconds(
                        retry_index,
                        retry_backoff_seconds=self._retry_backoff_seconds,
                        retry_after_seconds=None,
                    )
                    if delay > 0:
                        await asyncio.sleep(delay)
                    continue

                raise Undr9ConnectionError(
                    base_url=self._base_url,
                    reason=str(error),
                ) from error

            if not response.content:
                return None
            return response.json()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def text_request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> str:
        retryable_request = _request_is_retryable(
            method,
            path,
            retry_non_idempotent_requests=self._retry_non_idempotent_requests,
        )

        for retry_index in range(self._max_retries + 1):
            try:
                response = await self._client.request(method=method, url=path, json=payload)
                response.raise_for_status()
                return response.text
            except httpx.HTTPStatusError as error:
                should_retry = (
                    retryable_request
                    and retry_index < self._max_retries
                    and error.response.status_code in self._retry_status_codes
                )
                if should_retry:
                    delay = _retry_delay_seconds(
                        retry_index,
                        retry_backoff_seconds=self._retry_backoff_seconds,
                        retry_after_seconds=_retry_after_seconds(error.response.headers),
                    )
                    if delay > 0:
                        await asyncio.sleep(delay)
                    continue
                code, message, details = _parse_error_payload(
                    error.response.text,
                    fallback_message=error.response.reason_phrase,
                )
                raise Undr9ApiError(
                    status_code=error.response.status_code,
                    code=code,
                    message=message,
                    details=details,
                ) from error
            except httpx.RequestError as error:
                should_retry = retryable_request and retry_index < self._max_retries
                if should_retry:
                    delay = _retry_delay_seconds(
                        retry_index,
                        retry_backoff_seconds=self._retry_backoff_seconds,
                        retry_after_seconds=None,
                    )
                    if delay > 0:
                        await asyncio.sleep(delay)
                    continue
                raise Undr9ConnectionError(
                    base_url=self._base_url,
                    reason=str(error),
                ) from error

    async def stream_request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        retryable_request = _request_is_retryable(
            method,
            path,
            retry_non_idempotent_requests=self._retry_non_idempotent_requests,
        )
        yielded_any = False

        for retry_index in range(self._max_retries + 1):
            try:
                async with self._client.stream(method=method, url=path, json=payload) as response:
                    response.raise_for_status()
                    async for raw_line in response.aiter_lines():
                        parsed = _parse_ndjson_line(raw_line)
                        if parsed is None:
                            continue
                        yielded_any = True
                        yield parsed
                    return
            except httpx.HTTPStatusError as error:
                should_retry = (
                    not yielded_any
                    and retryable_request
                    and retry_index < self._max_retries
                    and error.response.status_code in self._retry_status_codes
                )
                if should_retry:
                    delay = _retry_delay_seconds(
                        retry_index,
                        retry_backoff_seconds=self._retry_backoff_seconds,
                        retry_after_seconds=_retry_after_seconds(error.response.headers),
                    )
                    if delay > 0:
                        await asyncio.sleep(delay)
                    continue

                code, message, details = _parse_error_payload(
                    error.response.text,
                    fallback_message=error.response.reason_phrase,
                )
                raise Undr9ApiError(
                    status_code=error.response.status_code,
                    code=code,
                    message=message,
                    details=details,
                ) from error
            except httpx.RequestError as error:
                should_retry = (
                    not yielded_any and retryable_request and retry_index < self._max_retries
                )
                if should_retry:
                    delay = _retry_delay_seconds(
                        retry_index,
                        retry_backoff_seconds=self._retry_backoff_seconds,
                        retry_after_seconds=None,
                    )
                    if delay > 0:
                        await asyncio.sleep(delay)
                    continue

                raise Undr9ConnectionError(
                    base_url=self._base_url,
                    reason=str(error),
                ) from error
