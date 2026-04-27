import msgspec.json

from scm.rpc.types import ActionAttributes


def test_credentials_level_is_optional_in_action_attributes():
    """Required for backward compatibility, when deploying the RPC server before clients."""
    encoded = """{"method": "GET", "path": "/path/to/resource", "headers": {"X-Foo": "Bar"},
        "data": {}, "params": {"baz": "bat"}, "allow_redirects": true, "stream": false}"""
    assert msgspec.json.decode(encoded, type=ActionAttributes) == ActionAttributes(
        method="GET",
        path="/path/to/resource",
        headers={"X-Foo": "Bar"},
        data={},
        params={"baz": "bat"},
        allow_redirects=True,
        stream=False,
        credentials_level="installation",
    )
