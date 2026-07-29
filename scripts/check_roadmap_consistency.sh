#!/usr/bin/env bash
# Verify docs/ROADMAP.md agrees with the GitHub state it describes.
#
# The index is read by future sessions as authoritative. An inconsistency between
# it and the tracker is inherited as fact, so it has to be checkable rather than
# promised. Run this after editing the index, after refining a milestone, and
# before trusting anything the index says.
#
# Requires: gh, authenticated. Exits non-zero on any mismatch.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 2

INDEX=docs/ROADMAP.md
REPO=$(gh repo view --json nameWithOwner -q .nameWithOwner) || exit 2
fail=0
note() { printf '  %-6s %s\n' "$1" "$2"; }

echo "Checking $INDEX against $REPO"

echo
echo "1. Each milestone's acceptance criterion is identical in index, milestone and refinement issue"
while read -r num; do
  title=$(gh api "repos/$REPO/milestones/$num" --jq '.title')
  crit=$(gh api "repos/$REPO/milestones/$num" --jq '.description' | sed -n 's/^Acceptance: //p')
  if [ -z "$crit" ]; then
    note FAIL "milestone $num ($title) has no 'Acceptance:' line"; fail=1; continue
  fi
  grep -qF "$crit" "$INDEX" || { note FAIL "milestone $num criterion absent from $INDEX"; fail=1; }

  # the refinement issue for this milestone quotes the criterion as a blockquote
  issue=$(gh issue list --repo "$REPO" --milestone "$title" --label refinement \
            --state all --json number --jq '.[0].number')
  if [ -z "$issue" ] || [ "$issue" = "null" ]; then
    note FAIL "milestone $num ($title) has no refinement issue"; fail=1; continue
  fi
  quoted=$(gh issue view "$issue" --repo "$REPO" --json body --jq .body | sed -n 's/^> //p' | head -1)
  [ "$quoted" = "$crit" ] || { note FAIL "issue #$issue quotes a different criterion than milestone $num"; fail=1; }
  [ $fail -eq 0 ] && note ok "milestone $num = issue #$issue = index"
done < <(gh api "repos/$REPO/milestones" --jq '.[].number')

echo
echo "2. No milestone carries a due date"
n=$(gh api "repos/$REPO/milestones" --jq '[.[] | select(.due_on != null)] | length')
[ "$n" = "0" ] && note ok "none" || { note FAIL "$n milestone(s) have a due date"; fail=1; }

echo
echo "3. Every label the index names exists"
for l in refinement research feature ready; do
  grep -q "\`$l\`" "$INDEX" || continue
  gh label list --limit 100 --json name --jq '.[].name' | grep -qx "$l" \
    && note ok "$l" || { note FAIL "index names label '$l', which does not exist"; fail=1; }
done

echo
echo "4. Label descriptions state no rule (the index owns the rules)"
if gh label list --limit 100 --json name,description \
     --jq '.[] | select(.name|test("^(refinement|research|feature|ready)$")) | .description' \
   | grep -qiE "done when|authorises|authorizes|covers|produces|belongs to"; then
  note FAIL "a label description states a rule; it will drift from the index"; fail=1
else
  note ok "bare identifiers only"
fi

echo
echo "5. CLAUDE.md points at the index rather than restating its rules"
for r in ready feature refinement "graph wins" shaped frozen appetite; do
  grep -qi "$r" CLAUDE.md && { note FAIL "CLAUDE.md mentions '$r' — rules belong in $INDEX"; fail=1; }
done
[ $fail -eq 0 ] && note ok "pointer only"

echo
if [ $fail -eq 0 ]; then echo "CONSISTENT"; else echo "INCONSISTENT — see FAIL lines above"; fi
exit $fail
