#!/bin/bash
# Build a Slack webhook payload announcing a release, with the changelog of
# commits since the previous release tag. Prints JSON to stdout.
#
# Usage: release-notify.sh TAG RELEASE_URL REPO_URL
set -euo pipefail

TAG="${1}"
RELEASE_URL="${2}"
REPO_URL="${3}"
MAX_CHARS=3500

# Previous release = the highest version tag below TAG.
PREV_TAG=$(git tag --sort=-v:refname | grep -E '^[0-9]+\.[0-9]+\.[0-9]+$' |
  awk -v tag="$TAG" 'found { print; exit } $0 == tag { found = 1 }')

if [ -n "$PREV_TAG" ]; then
  RANGE="${PREV_TAG}..${TAG}"
  COMPARE="<${REPO_URL}/compare/${PREV_TAG}...${TAG}|${PREV_TAG}...${TAG}>"
else
  RANGE="${TAG}"
  COMPARE="first release"
fi

# Skip merge commits and craft's own "release: X.Y.Z" bump commits. Escape
# Slack control characters, then link PR references like (#123).
CHANGES=$(git log --no-merges --format='%s|%an' "$RANGE" |
  grep -vE '^release: [0-9]+\.[0-9]+\.[0-9]+\|' |
  sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g' |
  sed -E "s@\(#([0-9]+)\)@(<${REPO_URL}/pull/\1|#\1>)@g" |
  awk -F'|' '{ author = $NF; sub(/\|[^|]*$/, ""); printf "• %s — %s\n", $0, author }' || true)

if [ -z "$CHANGES" ]; then
  CHANGES="_No changes found._"
elif [ "${#CHANGES}" -gt "$MAX_CHARS" ]; then
  CHANGES="$(printf '%s' "$CHANGES" | head -c "$MAX_CHARS" | sed '$d')"$'\n'"… (truncated, see release)"
fi

REPO_NAME="${REPO_URL#https://github.com/}"
TEXT=$(printf '*:rocket: %s <%s|%s> released*\nChanges since %s:\n%s' \
  "$REPO_NAME" "$RELEASE_URL" "$TAG" "$COMPARE" "$CHANGES")

jq -n --arg text "$TEXT" '{text: $text, unfurl_links: false, unfurl_media: false}'
