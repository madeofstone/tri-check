# Agent workspace

Working documents for anyone — human or agent — picking up this codebase.

Unlike the usual local-only agent workspace, **these files are committed**. They
are meant to travel with the branch so a customer or another agent gets the same
context without asking.

## Read in this order

| File | What it gives you |
|---|---|
| [architecture.md](architecture.md) | What the app does, where the code lives, the data model, and the platform behaviours you cannot guess from the source. **Start here.** |
| [conventions.md](conventions.md) | How to make changes without breaking things that look fine |
| [decisions.md](decisions.md) | Why the design is the way it is, with the measurements behind each choice |
| [handoff.md](handoff.md) | Current state, open questions, session history |

For a discrete piece of work, copy [features/TEMPLATE](features/TEMPLATE) to
`features/<your-feature>/` and keep goals, strategy, decisions and handoff there.

## Session start

1. `git branch --show-current` — know which branch you are on before editing
2. Read `architecture.md` and `handoff.md` in full; skim `decisions.md`
3. Say what you are about to do before you start

## Session end

1. Overwrite **Current State** in `handoff.md`; append a dated session entry
2. Append any non-obvious choice to `decisions.md` — including the number or
   observation that drove it
3. Commit on the right branch, conventional prefix, explaining *why*. Do not
   push unless asked

## The one habit that matters

This app talks to two APIs whose field names lie. Verify behaviour against the
live tenant before building on an assumption, and put the measurement in the
commit message. `conventions.md` lists the specific ways this has gone wrong.
