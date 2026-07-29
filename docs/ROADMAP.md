# pymusicbrainz roadmap

> **This is agent-authored synthesis, not a record of decisions.** It was written by Claude Code in conversation with the maintainer. Items tagged `stated` are the maintainer's; everything else is the agent's reading and is open to revision at any time. Do not cite it back at the maintainer as their own decision.

This document holds **direction, order and known unknowns**. It does not hold *how*, and it does not hold state.

Progress lives in GitHub: [milestones](https://github.com/pvliesdonk/pymusicbrainz/milestones) and issues track what is open, closed and blocked. Nothing in this file counts, estimates or reports status — that would churn constantly and go stale invisibly.

**Nothing here is executable.** A milestone is an idea. Each milestone carries a refinement issue; until that produces features, the milestone is direction only.

## How this roadmap is kept

This section is the single definition of the mechanics. `CLAUDE.md` points here and restates none of it, so the two cannot drift apart.

| Label | Kind of work | Done when |
|---|---|---|
| `refinement` | Shape a milestone into features | Features exist that cover the milestone's acceptance criterion |
| `research` | Answer a question; carries an appetite | The question is answered |
| `feature` | Delivery work | The software works |
| `ready` | — | *Not a kind of work.* The gate: **the only label that authorises building** |

**Work starts from an issue labelled `ready`, or it does not start.** The gate is a label rather than a judgement so an independent reader can test it.

**A milestone with no `feature` issues has not been shaped**, and nothing may be built from it. Counting open issues is not the test — a milestone can carry research spikes alongside its refinement issue and still be unshaped.

When the issue graph comes to imply an order this document argues against, **the graph wins**: rewrite the argument here rather than bending the graph to match the prose.

## Provenance key

| Tag | Meaning | How it may be changed |
|---|---|---|
| `stated` | The maintainer said it | Never revised unilaterally. If evidence contradicts it, surface the contradiction — do not edit around it |
| `derived` | Agent synthesis | Revise in place at any refinement — **except** milestone acceptance criteria, which are frozen once refinement of their milestone begins (see *Milestones*) |
| `evidenced` | Read from the code or repo | Re-check the locator on revisit; if it no longer resolves, the item is `derived` again |

## The ambition

Three things, and the maintainer's position is that the *order* is the open question, not the goals. `stated`

- **Availability** — the library keeps working when the self-hosted infrastructure does not. `stated`
- **Trustworthiness** — its answers are right, and it is honest when it cannot know. `stated`
- **Maintainability** — the code can be changed without fear, and without carrying a forked dependency. `stated`

Constraints: hobby pace, no deadline. No public consumers; the one downstream consumer is the maintainer's own and will be overhauled separately. No backward compatibility is required or wanted. `stated`

Determinism is **a wish, not a requirement**. `stated`

## Milestones

Each acceptance criterion is an outcome, written before any feature existed, and is **frozen through refinement**. It is the independent check on decomposition; rewriting it to match whatever features get created destroys its only function. A milestone closes when its criterion is met, not when its issue list empties.

| Milestone | Acceptance criterion | Prov. |
|---|---|---|
| [A — Honest failure](https://github.com/pvliesdonk/pymusicbrainz/milestone/1) | A caller can tell the difference between "there is no such recording" and "I could not reach something", without reading logs. | `derived` |
| [B — Ground truth](https://github.com/pvliesdonk/pymusicbrainz/milestone/2) | Someone can change how results are chosen and find out, without a database, whether the answers got better or worse. | `derived` |
| [C — Canonical matching without a server](https://github.com/pvliesdonk/pymusicbrainz/milestone/3) | Canonical matching works on a machine running no services, from an artefact that can be re-downloaded. | `derived` |
| [D — A reviewable search policy](https://github.com/pvliesdonk/pymusicbrainz/milestone/4) | A reader can follow how a release is chosen, and disagree with it specifically. | `derived` |
| [E — Retire the forked dependency](https://github.com/pvliesdonk/pymusicbrainz/milestone/5) | MusicBrainz schema updates can be adopted without maintaining a fork. | `derived` |
| [F — Works without the mirror](https://github.com/pvliesdonk/pymusicbrainz/milestone/6) | The library answers the ground-truth corpus with no self-hosted infrastructure; every result produced that way is marked as such; and any way its answers differ from the mirror's is recorded rather than discovered later. | `derived` |
| [G — Coherent albums](https://github.com/pvliesdonk/pymusicbrainz/milestone/7) | Tagging a record yields one release, not several. | `derived` |

No milestone carries a due date. Milestones can arrive sooner, be postponed, or run in parallel with features woven between them, so a total order over them is a claim without evidence behind it.

## The order, and why

The argument is **information gain**: do the thing first that most cheaply tells you what the rest are worth. Ordering by dependency alone produces a schedule; ordering by what it teaches you produces a roadmap.

**A first.** It is the cheapest thing that changes what everything else *means*. While an unreachable source is indistinguishable from an empty result, every later measurement can be silently wrong about *why* something failed — and a baseline recorded on a day when something was quietly down encodes that breakage as the definition of normal. `derived`

**B second.** It resolves the single largest unknown under every remaining milestone: what share of bad answers is fixable here at all, and by which one. D, F and G are each a wager on that share, and none of them can be sized before it is known. `derived`

**C third**, subject to its spike. It cheaply resolves how much of the current answer quality is explained by the missing canonical seed. If restoring it moves quality a long way, D shrinks; if it does not, D grows. Either answer is worth having before committing to D. `derived`

**D, E, F and G are deliberately left unordered.** There is no evidence for a total order over them, and A–C are chosen precisely to produce that evidence. E is the odd one out: it resolves nothing and is pure maintenance debt, so at hobby pace it can be woven in when it becomes annoying rather than sequenced. `derived`

### A finding from charting

**F is what this work was originally asked for, and it lands last.** Everything else turned out to be load-bearing before it can even be shaped sensibly: without A its degradation is unreportable, without B there is no way to tell whether it answers correctly, and its central unknown — whether an API-backed implementation can reproduce the mirror's results at all — cannot be judged until B exists.

That is a result in its own right, and it is recorded here because it is exactly the kind of conclusion that gets quietly re-litigated later. The alternative ordering — by observed daily pain rather than information gain — would put G much earlier. That remains a legitimate direction call rather than an error. `derived`

### Order that the graph carries instead

Executable, structural edges live in GitHub as blocked-by relationships on the issues themselves. They are not restated here, because this file would then carry a status that goes stale invisibly the moment the edge is discharged.

One edge cannot be created yet: recording B's baseline should not happen before A's behaviour lands. Both milestones are still ideas, so there are no features to connect.

**Whichever of A and B is refined second owns creating that edge**, because only then do both endpoints exist. It is a done-when condition on that refinement issue, not an argument to be remembered.

## Known unknowns

Every unknown carries a pointer to the work whose completion answers it. An unknown without one is a worry, not structure.

| Unknown | Resolved by | Prov. |
|---|---|---|
| What share of wrong answers comes from the MusicBrainz graph's incompleteness — which nothing here can fix — versus from how results are chosen, which is fixable? | **B** | premise `stated`, framing `derived` |
| How much of the current answer quality is explained by the canonical seed being unavailable? | **C** | `derived` |
| Is an embedded canonical index viable at the published dump's real scale? | [spike #14](https://github.com/pvliesdonk/pymusicbrainz/issues/14) | `derived` |
| Can an API-backed implementation reproduce the mirror's release-group sets at all? The mirror answers these from a precomputed table that has no equivalent in the web API. | refinement of **F** | `evidenced` — `pymusicbrainz/dataclasses.py` queries `mbdata.models.ArtistReleaseGroup`, a MusicBrainz materialised table; read 2026-07-29 |
| Is normalised exact matching enough, or is fuzzy matching load-bearing? | **B** | `derived` |
| Would embeddings or vector search beat exact-plus-fuzzy matching? | whether fuzzy matching turns out to be load-bearing (**B**); nothing is planned before that answer | `stated` — raised by the maintainer |
| How much run-to-run variance is there actually? It has never been measured. | **B** | `derived` |
| Is the library's editorial idea of the "right" release the one the maintainer actually wants? Writing expectations by hand is the act of deciding this. | **B** | `derived` |

Unknowns that do not change what happens next are recorded here and left unticketed on purpose. Ticketing every one of them would fill the tracker with planning work and drown the delivery signal.

## Revisiting

A roadmap that is not maintained is worse than none, because it is confidently wrong. On returning:

- What closed, and which unknowns did it resolve?
- Does the issue graph now imply an order this argument contradicts? **The graph wins** — rewrite the argument here, never bend the graph to match the prose.
- Did anything get postponed, and why? Record the reason.
- Which `derived` items are now `evidenced`, or now wrong? Every closed research issue should have moved something in this file. One that closed without changing the argument produced nothing.
- Did any research issue exceed its appetite? That is a finding about the direction, not a request for more time.
- For any milestone whose issues are all closed: is its acceptance criterion actually met? An empty issue list is not delivery.
- Does any evidence now contradict a `stated` item? Surface it; do not edit around it.

Defects and things that are simply wrong are **not roadmap work**. They are filed as ordinary bugs and do not belong in this document.
