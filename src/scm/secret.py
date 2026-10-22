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


def require_secret(value: object, *, parameter: str) -> Secret:
    """Return ``value`` if it is a ``Secret``.

    Type checkers catch a plain string already; this tells an untyped caller what to do instead of failing later
    with an AttributeError.
    """
    if not isinstance(value, Secret):
        # A plain string would stay in the caller's frame, where an error report shows it.
        raise TypeError(
            f"{parameter} must be a scm.Secret, not {type(value).__name__}. "
            'Wrap it in scm.Secret where you read it, e.g. Secret(os.environ["SCM_RPC_SHARED_SECRET"]).'
        )
    return value


def require_secrets(values: Sequence[object], *, parameter: str) -> list[Secret]:
    """Like ``require_secret``, for each of ``values``."""
    return [require_secret(value, parameter=parameter) for value in values]
