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
