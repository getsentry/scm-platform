from collections.abc import Sequence


class Secret:
    """A string that must not reach a log line or an error report, such as a signing secret.

    Sentry's SDK sends the local variables of every frame in a reported traceback, and its default scrubbing only
    replaces variables with a few exact names (``secret`` is one, ``secrets`` and ``signing_secret`` are not), never
    looking inside a list or a dict. Held in a ``Secret``, the value shows as ``[Filtered]`` wherever it is: the SDK
    calls ``__sentry_repr__`` first, and ``repr()`` and ``str()`` hide it too.

    ``reveal()`` returns the plain string, which is then in the frame that called it. Call it where the value is used,
    inline, rather than keeping the result in a variable.
    """

    __slots__ = ("_value",)

    def __init__(self, value: str) -> None:
        self._value = value

    def reveal(self) -> str:
        return self._value

    def __repr__(self) -> str:
        return "Secret('[Filtered]')"

    def __str__(self) -> str:
        return "[Filtered]"

    def __sentry_repr__(self) -> str:
        return "[Filtered]"

    def __bool__(self) -> bool:
        # False for an empty secret, as for an empty string, so a check such as ``if not secret`` still refuses one.
        return bool(self._value)


def require_secrets(values: Sequence[object], *, parameter: str) -> list[Secret]:
    """Return ``values`` as a list, if each is a ``Secret``.

    For the servers, which hold their secrets for every request. A plain string that got past the type checker, say
    from untyped code, would make every request fail inside verification, and each failure's error report would
    carry the strings in its frames. Refused here, the server fails once, when it is built.
    """
    secrets: list[Secret] = []
    for value in values:
        if not isinstance(value, Secret):
            raise TypeError(
                f"{parameter} must be scm.Secret objects, not {type(value).__name__}. "
                'Wrap each in scm.Secret where you read it, e.g. Secret(os.environ["SCM_RPC_SHARED_SECRET"]).'
            )
        secrets.append(value)
    return secrets
