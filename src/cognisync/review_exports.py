from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Dict, List, Optional

from cognisync.review_queue import build_review_queue
from cognisync.review_state import read_review_actions
from cognisync.types import IndexSnapshot
from cognisync.utils import utc_timestamp
from cognisync.workspace import Workspace


@dataclass(frozen=True)
class ReviewExportResult:
    path: Path
    item_count: int
    dismissed_count: int


def write_review_export(
    workspace: Workspace,
    snapshot: IndexSnapshot,
    output_file: Optional[Path] = None,
) -> ReviewExportResult:
    payload = build_review_export_payload(workspace, snapshot)
    path = output_file or _next_review_export_path(workspace)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return ReviewExportResult(
        path=path,
        item_count=int(payload["summary"]["open_item_count"]),
        dismissed_count=int(payload["summary"]["dismissed_item_count"]),
    )


def build_review_export_payload(workspace: Workspace, snapshot: IndexSnapshot) -> Dict[str, object]:
    queue = build_review_queue(workspace, snapshot)
    actions = read_review_actions(workspace)
    open_items = list(queue.get("items", []))
    dismissed_items = _dismissed_review_items(actions)
    open_item_counts_by_kind = dict(Counter(str(item.get("kind", "")) for item in open_items))
    action_state = {
        "accepted_concepts": dict(actions.get("accepted_concepts", {})),
        "applied_backlinks": dict(actions.get("applied_backlinks", {})),
        "dismissed_reviews": dict(actions.get("dismissed_reviews", {})),
        "filed_conflicts": dict(actions.get("filed_conflicts", {})),
        "resolved_entity_merges": dict(actions.get("resolved_entity_merges", {})),
    }
    return {
        "schema_version": 1,
        "generated_at": utc_timestamp(),
        "snapshot_generated_at": snapshot.generated_at,
        "workspace": {
            "root": workspace.root.as_posix(),
            "review_queue_manifest_path": workspace.relative_path(workspace.review_queue_manifest_path),
            "review_actions_manifest_path": workspace.relative_path(workspace.review_actions_manifest_path),
        },
        "summary": {
            "open_item_count": len(open_items),
            "dismissed_item_count": len(dismissed_items),
            "open_item_counts_by_kind": open_item_counts_by_kind,
            "action_counts": {
                "accepted_concepts": len(action_state["accepted_concepts"]),
                "applied_backlinks": len(action_state["applied_backlinks"]),
                "dismissed_reviews": len(action_state["dismissed_reviews"]),
                "filed_conflicts": len(action_state["filed_conflicts"]),
                "resolved_entity_merges": len(action_state["resolved_entity_merges"]),
            },
        },
        "open_items": open_items,
        "dismissed_items": dismissed_items,
        "action_state": action_state,
    }


def _dismissed_review_items(actions: Dict[str, object]) -> List[Dict[str, object]]:
    items: List[Dict[str, object]] = []
    for review_id, entry in sorted(dict(actions.get("dismissed_reviews", {})).items()):
        payload = dict(entry)
        payload["review_id"] = str(review_id)
        items.append(payload)
    return items


def _next_review_export_path(workspace: Workspace) -> Path:
    directory = workspace.review_exports_dir
    directory.mkdir(parents=True, exist_ok=True)
    stem = f"review-export-{_filename_timestamp(utc_timestamp())}"
    candidate = directory / f"{stem}.json"
    index = 2
    while candidate.exists():
        candidate = directory / f"{stem}-{index}.json"
        index += 1
    return candidate


def _filename_timestamp(value: str) -> str:
    return value.replace(":", "").replace("-", "").replace("+", "Z").replace(".", "")
