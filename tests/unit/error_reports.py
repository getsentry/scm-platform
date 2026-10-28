def assert_not_in_frames(error: BaseException, value: str) -> None:
    """Fail if any frame of the error's traceback holds ``value`` where an error report would show it.

    Sentry's SDK sends every frame's local variables, using ``__sentry_repr__`` or else ``repr()`` for each, and its
    default scrubbing only replaces variables with a few exact names, such as ``secret``.
    """
    tb = error.__traceback__
    while tb is not None:
        for name, local in tb.tb_frame.f_locals.items():
            assert value not in _shown(local), f"{tb.tb_frame.f_code.co_name} holds it in {name}"
        tb = tb.tb_next


def reported_local(error: BaseException, function: str, name: str) -> str:
    """How an error report shows the local variable ``name`` of ``function``'s frame in the error's traceback."""
    tb = error.__traceback__
    while tb is not None:
        if tb.tb_frame.f_code.co_name == function:
            return _shown(tb.tb_frame.f_locals[name])
        tb = tb.tb_next
    raise AssertionError(f"The traceback has no frame of {function}.")


def _shown(local: object) -> str:
    return local.__sentry_repr__() if hasattr(local, "__sentry_repr__") else repr(local)
