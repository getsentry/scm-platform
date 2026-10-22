import warnings
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

    @classmethod
    def of(cls, value: "str | Secret") -> "Secret":
        """Wrap ``value``, unless it already is a ``Secret``."""
        return value if isinstance(value, Secret) else cls(value)

    def reveal(self) -> str:
        return self._value

    def __repr__(self) -> str:
        return "Secret('[Filtered]')"

    def __str__(self) -> str:
        return "[Filtered]"

    def __sentry_repr__(self) -> str:
        return "[Filtered]"


def accept_secret(value: str | Secret, *, parameter: str) -> Secret:
    """Wrap a secret given to a public entry point, warning if it came as a plain string.

    Call it first thing in the entry point, so the warning points at the entry point's caller.
    """
    if isinstance(value, str):
        _warn_plain_string(parameter)
    return Secret.of(value)


def accept_secrets(values: Sequence[str | Secret], *, parameter: str) -> list[Secret]:
    """Like ``accept_secret``, for a list of secrets, warning once however many are plain strings."""
    if any(isinstance(value, str) for value in values):
        _warn_plain_string(parameter)
    return [Secret.of(value) for value in values]


def _warn_plain_string(parameter: str) -> None:
    # A plain string stays in the caller's frame, where an error report shows it, so callers should wrap it where they
    # read it. Accepted until the next major release, so Sentry and Seer can move over first.
    warnings.warn(
        f"Passing {parameter} as a plain str is deprecated and will stop working in the next major release. "
        'Wrap it in scm.Secret where you read it, e.g. Secret(os.environ["SCM_RPC_SHARED_SECRET"]).',
        DeprecationWarning,
        # This function, accept_secret(s), the entry point, then the entry point's caller.
        stacklevel=4,
    )
