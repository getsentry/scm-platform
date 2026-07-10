from scm.providers.routes import FormattedRoute, Route


class TestRoute:
    def test_formats_path_and_preserves_template(self):
        result = Route("/repos/{repo}/issues/{issue_id}")(repo="getsentry/sentry", issue_id=5)
        assert result == FormattedRoute(
            path="/repos/getsentry/sentry/issues/5",
            template="/repos/{repo}/issues/{issue_id}",
        )

    def test_static_route_formats_to_itself(self):
        assert Route("/app")() == FormattedRoute(path="/app", template="/app")

    def test_slash_containing_param_lands_in_path_but_template_stays_stable(self):
        # The reliability property this whole design exists for: a value containing slashes lands
        # verbatim in ``path`` while ``template`` remains low-cardinality — the case that makes
        # reverse-parsing an already-interpolated path ambiguous.
        result = Route("/repos/{repo}/contents/{path}")(repo="o/r", path="src/app/main.py")
        assert result.path == "/repos/o/r/contents/src/app/main.py"
        assert result.template == "/repos/{repo}/contents/{path}"

    def test_formatted_routes_are_equal_by_value(self):
        route = Route("/projects/{project_id}/merge_requests/{pr_key}")
        assert route(project_id=1, pr_key=2) == route(project_id=1, pr_key=2)
        assert route(project_id=1, pr_key=2) != route(project_id=1, pr_key=3)
