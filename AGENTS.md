# Repository agent conventions

## When changing the provider protocol (`src/scm/types.py`)

If you add or change a parameter on a method in `CreateCommitProtocol` or any
other provider protocol, you MUST update **all** of the following surfaces so
the abstraction stays symmetric across providers and tools:

1. `src/scm/actions.py` — forward the parameter through the action wrapper.
2. `src/scm/providers/github/provider.py` — GitHub implementation.
3. `src/scm/providers/gitlab/provider.py` — GitLab implementation.
4. `src/scm/test_fixtures.py` — `FakeProvider` signature.
5. `bin/github-client` and `bin/gitlab-client` — CLI usage docstring,
   `argparse` definition, and the call site that invokes the provider.
6. Tests under `tests/unit/provider/test_github.py` and
   `tests/unit/provider/test_gitlab.py` covering each branch of the new
   parameter.

Run `uv run pytest tests/`, `uv run ruff check src tests`, and
`uv run mypy src` before committing.
