# Strategy: <feature-name>

## Approach
<The shape of the change, and which layers it touches: sync, db, API, UI.>

## Key components
<Specific files and functions to add or change. Name them.>

| File | Change |
|---|---|
| `db.py` | <e.g. new query + migration> |
| `app.py` | <e.g. new endpoint> |
| `overview.js` | <e.g. new panel> |

## Sequencing
<What to build first and what depends on it. Backend before UI, usually — the
API can be verified on its own.>

## Risks and unknowns
<What might not behave as assumed. Anything needing a live API probe first —
see ../../conventions.md.>

## Verification
<How this gets checked: which endpoints, which browser interaction, which
numbers should move and to what.>
