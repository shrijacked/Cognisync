from __future__ import annotations

from collections import defaultdict
import re
from typing import Dict, List, Sequence, Set, Tuple

from cognisync.types import ArtifactRecord, IndexSnapshot
from cognisync.utils import slugify
from cognisync.workspace import Workspace


TEXTUAL_KINDS = {"markdown", "text", "data", "code"}
TOKEN_RE = re.compile(r"[a-z0-9]+")
ENTITY_RE = re.compile(
    r"\b(?:[A-Z][A-Za-z0-9]+|[A-Z]{2,})(?:\s+(?:[A-Z][A-Za-z0-9]+|[A-Z]{2,})){1,3}\b"
)
CLAIM_RE = re.compile(
    r"^\s*(?:[-*]\s+)?([A-Za-z][A-Za-z0-9 /_-]{2,60}?)\s+"
    r"(is|are|uses|supports|requires|prefers)\s+"
    r"([A-Za-z][A-Za-z0-9 /_-]{2,80}?)(?:[.!?]|$)",
    re.IGNORECASE,
)
ENTITY_STOPWORDS = {
    "Code Of Conduct",
    "Compile Plan",
    "Content",
    "Description",
    "Discovered Links",
    "Extracted Metadata",
    "Extracted Text",
    "Generated Artifacts",
    "Knowledge Base Index",
    "Prompt Hint",
    "Prompt Packet",
    "Queries",
    "Readme Excerpt",
    "Recent Commits",
    "Related Concepts",
    "Repository Stats",
    "Research Brief",
    "Research Plan",
    "Sections",
    "Source Blocks",
    "Source Context",
    "Source File",
    "Source Url",
    "Sources",
    "Top Sources",
}


def strip_frontmatter(text: str) -> str:
    if not text.startswith("---\n"):
        return text
    end = text.find("\n---\n", 4)
    if end == -1:
        return text
    return text[end + 5 :]


def extract_claim_tuples(text: str) -> List[Tuple[str, str, str]]:
    body = strip_frontmatter(text)
    claims: List[Tuple[str, str, str]] = []
    in_code = False
    for raw_line in body.splitlines():
        line = raw_line.strip()
        if line.startswith("```"):
            in_code = not in_code
            continue
        if in_code or not line or line.startswith("#"):
            continue
        match = CLAIM_RE.match(line)
        if not match:
            continue
        subject = " ".join(match.group(1).lower().split())
        verb = match.group(2).lower()
        obj = " ".join(match.group(3).lower().split())
        claims.append((subject, verb, obj))
    return claims


def build_concept_candidates(snapshot: IndexSnapshot) -> List[Dict[str, object]]:
    support: Dict[str, Dict[str, object]] = {}
    existing_paths = set(snapshot.artifact_paths())
    for artifact in snapshot.artifacts:
        if artifact.collection not in {"raw", "wiki"} or artifact.kind != "markdown":
            continue
        for label, evidence_kind in _candidate_labels_from_artifact(artifact):
            canonical = canonicalize_graph_label(label)
            if not canonical:
                continue
            entry = support.setdefault(
                canonical,
                {
                    "labels": set(),
                    "evidence_kinds": set(),
                    "support_paths": set(),
                },
            )
            entry["labels"].add(label)
            entry["evidence_kinds"].add(evidence_kind)
            entry["support_paths"].add(artifact.path)

    candidates: List[Dict[str, object]] = []
    for canonical, data in sorted(support.items()):
        support_paths = sorted(data["support_paths"])
        if len(support_paths) < 2:
            continue
        title = _preferred_graph_label(data["labels"])
        slug = slugify(title)
        output_path = f"wiki/concepts/{slug}.md"
        candidates.append(
            {
                "id": f"concept:{slug}",
                "slug": slug,
                "title": title,
                "support_count": len(support_paths),
                "support_paths": support_paths,
                "evidence_kinds": sorted(data["evidence_kinds"]),
                "output_path": output_path,
                "resolved": output_path in existing_paths,
            }
        )
    return candidates


def build_graph_semantics(workspace: Workspace, snapshot: IndexSnapshot) -> Dict[str, List[Dict[str, object]]]:
    entity_support: Dict[str, Dict[str, object]] = {}
    artifact_mentions: Dict[str, Set[str]] = defaultdict(set)
    conflict_edges: List[Dict[str, object]] = []
    conflict_seen: Set[Tuple[str, str, str, str]] = set()

    textual_artifacts = [
        artifact
        for artifact in snapshot.artifacts
        if artifact.kind in TEXTUAL_KINDS and artifact.collection in {"raw", "wiki", "outputs"}
    ]

    claim_support: Dict[Tuple[str, str], Dict[str, Set[str]]] = defaultdict(lambda: defaultdict(set))
    for artifact in textual_artifacts:
        text = _read_artifact_text(workspace, artifact)
        if not text:
            continue

        for label in _extract_entity_labels(artifact, text):
            slug = slugify(label)
            if not slug:
                continue
            entry = entity_support.setdefault(
                slug,
                {
                    "id": f"entity:{slug}",
                    "slug": slug,
                    "title": label,
                    "support_paths": set(),
                },
            )
            entry["support_paths"].add(artifact.path)
            artifact_mentions[artifact.path].add(slug)

        for subject, verb, obj in extract_claim_tuples(text):
            claim_support[(subject, verb)][obj].add(artifact.path)

    nodes: List[Dict[str, object]] = []
    edges: List[Dict[str, object]] = []

    for slug, data in sorted(entity_support.items()):
        support_paths = sorted(data["support_paths"])
        nodes.append(
            {
                "id": str(data["id"]),
                "kind": "entity",
                "slug": slug,
                "title": str(data["title"]),
                "support_count": len(support_paths),
            }
        )
    for artifact_path, entity_slugs in sorted(artifact_mentions.items()):
        for slug in sorted(entity_slugs):
            edges.append({"source": artifact_path, "target": f"entity:{slug}", "kind": "mentions"})

    for candidate in build_concept_candidates(snapshot):
        nodes.append(
            {
                "id": candidate["id"],
                "kind": "concept_candidate",
                "slug": candidate["slug"],
                "title": candidate["title"],
                "support_count": candidate["support_count"],
                "resolved": candidate["resolved"],
                "output_path": candidate["output_path"],
            }
        )
        for support_path in candidate["support_paths"]:
            edges.append({"source": support_path, "target": candidate["id"], "kind": "supports_concept"})

    for (subject, verb), object_map in sorted(claim_support.items()):
        if len(object_map) < 2:
            continue
        variants = sorted(object_map.items())
        for left_index, (left_value, left_paths) in enumerate(variants):
            for right_value, right_paths in variants[left_index + 1 :]:
                for left_path in sorted(left_paths):
                    for right_path in sorted(right_paths):
                        pair = tuple(sorted((left_path, right_path)))
                        key = (pair[0], pair[1], subject, verb)
                        if key in conflict_seen or pair[0] == pair[1]:
                            continue
                        conflict_seen.add(key)
                        conflict_edges.append(
                            {
                                "source": pair[0],
                                "target": pair[1],
                                "kind": "conflict",
                                "subject": subject,
                                "verb": verb,
                                "left_value": left_value,
                                "right_value": right_value,
                            }
                        )

    return {
        "nodes": nodes,
        "edges": edges + sorted(conflict_edges, key=lambda item: (item["source"], item["target"], item["subject"])),
    }


def _candidate_labels_from_artifact(artifact: ArtifactRecord) -> Set[Tuple[str, str]]:
    labels: Set[Tuple[str, str]] = set()
    for tag in artifact.tags:
        pretty = " ".join(part.capitalize() for part in tag.replace("_", "-").split("-") if part)
        if pretty:
            labels.add((pretty, "tag"))
    for label in [artifact.title, *artifact.headings]:
        normalized = _normalize_entity_label(label)
        if normalized is not None:
            labels.add((normalized, "entity"))
    return labels


def _extract_entity_labels(artifact: ArtifactRecord, text: str) -> Set[str]:
    labels: Set[str] = set()
    for label in [artifact.title, *artifact.headings]:
        normalized = _normalize_entity_label(label)
        if normalized is not None:
            labels.add(normalized)
    for match in ENTITY_RE.finditer(strip_frontmatter(text)):
        normalized = _normalize_entity_label(match.group(0))
        if normalized is not None:
            labels.add(normalized)
    return labels


def _normalize_entity_label(value: str) -> str | None:
    compact = " ".join(value.replace("_", " ").replace("-", " ").split()).strip()
    if len(compact) < 3:
        return None
    if compact in ENTITY_STOPWORDS:
        return None
    words = compact.split()
    if len(words) < 2:
        return None
    if len(words) > 4:
        return None
    if not all(_looks_named_token(word) for word in words):
        return None
    return " ".join(word if word.isupper() else word.capitalize() for word in words)


def _looks_named_token(word: str) -> bool:
    if word.isupper() and len(word) >= 2:
        return True
    return word[:1].isupper()


def canonicalize_graph_label(value: str) -> str:
    tokens = [token for token in TOKEN_RE.findall(value.lower()) if token]
    normalized = [_singularize_graph_token(token) for token in tokens]
    return " ".join(normalized)


def _preferred_graph_label(labels: Sequence[str]) -> str:
    return sorted(labels, key=lambda item: (-len(item.split()), -len(item), item))[0]


def _singularize_graph_token(token: str) -> str:
    if len(token) > 3 and token.endswith("ies"):
        return token[:-3] + "y"
    if len(token) > 3 and token.endswith("s") and not token.endswith("ss"):
        return token[:-1]
    return token


def _read_artifact_text(workspace: Workspace, artifact: ArtifactRecord) -> str:
    path = workspace.root / artifact.path
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="ignore")
