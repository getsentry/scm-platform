"""Reads and rewrites HTTP headers by name, whatever their spelling.

A mapping can hold one header under two spellings, and the relay passes on one of them, so every spelling counts.
"""

from collections.abc import Mapping


def header_values(headers: Mapping[str, str], name: str) -> list[str]:
    """Return the values of every spelling of ``name``, a lowercase header name."""
    return [value for key, value in headers.items() if key.lower() == name]


def without_header(headers: Mapping[str, str], name: str) -> dict[str, str]:
    """Return ``headers`` without any spelling of ``name``, a lowercase header name."""
    return {key: value for key, value in headers.items() if key.lower() != name}


def replacing_header(headers: Mapping[str, str], name: str, value: str) -> dict[str, str]:
    """Return ``headers`` with ``value`` as the only ``name`` header, whatever spelling it had."""
    return without_header(headers, name.lower()) | {name: value}
