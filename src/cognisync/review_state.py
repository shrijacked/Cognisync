from __future__ import annotations

import json
import re
from typing import Dict, Iterable

from cognisync.utils import utc_timestamp
from cognisync.workspace import Workspace


TOKEN_RE = re.compile(r"[a-z0-9]+")


def canonicalize_review_label(value: str) -> str:
    tokens = [token for token in TOKEN_RE.findall(normalize_review_label_variant(value).lower()) if token]
    while len(tokens) % 2 == 0 and tokens[: len(tokens) // 2] == tokens[len(tokens) // 2 :]:
        tokens = tokens[: len(tokens) // 2]
    normalized = [_singularize_token(token) for token in tokens]
    return " ".join(normalized)


def normalize_review_label_variant(value: str) -> str:
    words = [word for word in value.split() if word]
    while len(words) % 2 == 0 and [word.lower() for word in words[: len(words) // 2]] == [
        word.lower() for word in words[len(words) // 2 :]
    ]:
        words = words[: len(words) // 2]
    return " ".join(words)


def preferred_review_label(labels: Iterable[str]) -> str:
    options = sorted(
        {normalize_review_label_variant(label) for label in labels if normalize_review_label_variant(label).strip()},
        key=lambda item: (-len(item.split()), -len(item), item),
    )
    return options[0] if options else ""


def default_review_actions() -> Dict[str, object]:
    return {
        "schema_version": 1,
        "updated_at": utc_timestamp(),
        "accepted_concepts": {},
        "applied_backlinks": {},
        "dismissed_reviews": {},
        "filed_conflicts": {},
        "resolved_entity_merges": {},
    }


def read_review_actions(workspace: Workspace) -> Dict[str, object]:
    if not workspace.review_actions_manifest_path.exists():
        return default_review_actions()

    raw = json.loads(workspace.review_actions_manifest_path.read_text(encoding="utf-8"))
    payload = default_review_actions()
    payload["schema_version"] = int(raw.get("schema_version", 1))
    payload["updated_at"] = str(raw.get("updated_at", payload["updated_at"]))
    payload["accepted_concepts"] = {
        str(key): dict(value) for key, value in dict(raw.get("accepted_concepts", {})).items()
    }
    payload["applied_backlinks"] = {
        str(key): dict(value) for key, value in dict(raw.get("applied_backlinks", {})).items()
    }
    payload["dismissed_reviews"] = {
        str(key): {
            "kind": str(dict(value).get("kind", "")),
            "title": str(dict(value).get("title", "")),
            "path": str(dict(value).get("path", "")),
            "reason": str(dict(value).get("reason", "")),
            "related_paths": list(dict(value).get("related_paths", [])),
            "dismissed_at": str(dict(value).get("dismissed_at", "")),
        }
        for key, value in dict(raw.get("dismissed_reviews", {})).items()
    }
    payload["filed_conflicts"] = {
        str(key): dict(value) for key, value in dict(raw.get("filed_conflicts", {})).items()
    }
    payload["resolved_entity_merges"] = {
        str(key): {
            "preferred_label": str(dict(value).get("preferred_label", "")),
            "aliases": list(dict(value).get("aliases", [])),
            "related_paths": list(dict(value).get("related_paths", [])),
            "resolved_at": str(dict(value).get("resolved_at", "")),
        }
        for key, value in dict(raw.get("resolved_entity_merges", {})).items()
    }
    return payload


def write_review_actions(workspace: Workspace, payload: Dict[str, object]) -> None:
    actions = default_review_actions()
    actions["accepted_concepts"] = {
        str(key): dict(value) for key, value in dict(payload.get("accepted_concepts", {})).items()
    }
    actions["applied_backlinks"] = {
        str(key): dict(value) for key, value in dict(payload.get("applied_backlinks", {})).items()
    }
    actions["dismissed_reviews"] = {
        str(key): {
            "kind": str(dict(value).get("kind", "")),
            "title": str(dict(value).get("title", "")),
            "path": str(dict(value).get("path", "")),
            "reason": str(dict(value).get("reason", "")),
            "related_paths": sorted(
                {str(path) for path in list(dict(value).get("related_paths", [])) if str(path).strip()}
            ),
            "dismissed_at": str(dict(value).get("dismissed_at", "")),
        }
        for key, value in dict(payload.get("dismissed_reviews", {})).items()
    }
    actions["filed_conflicts"] = {
        str(key): dict(value) for key, value in dict(payload.get("filed_conflicts", {})).items()
    }
    actions["resolved_entity_merges"] = {
        str(key): {
            "preferred_label": str(dict(value).get("preferred_label", "")),
            "aliases": sorted({str(alias) for alias in list(dict(value).get("aliases", [])) if str(alias).strip()}),
            "related_paths": sorted({str(path) for path in list(dict(value).get("related_paths", [])) if str(path).strip()}),
            "resolved_at": str(dict(value).get("resolved_at", "")),
        }
        for key, value in dict(payload.get("resolved_entity_merges", {})).items()
    }
    actions["updated_at"] = utc_timestamp()
    workspace.review_actions_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    workspace.review_actions_manifest_path.write_text(json.dumps(actions, indent=2, sort_keys=True), encoding="utf-8")


def resolved_merge_preference(actions: Dict[str, object], label: str) -> str | None:
    canonical = canonicalize_review_label(label)
    entry = dict(actions.get("resolved_entity_merges", {})).get(canonical)
    if not entry:
        return None
    preferred = str(dict(entry).get("preferred_label", "")).strip()
    return preferred or None


def _singularize_token(token: str) -> str:
    if len(token) > 3 and token.endswith("ies"):
        return token[:-3] + "y"
    if len(token) > 3 and token.endswith("s") and not token.endswith("ss"):
        return token[:-1]
    return token
