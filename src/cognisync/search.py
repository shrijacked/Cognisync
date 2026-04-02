from __future__ import annotations

from dataclasses import dataclass
import math
import re
from typing import Dict, List

from cognisync.types import IndexSnapshot, SearchHit
from cognisync.workspace import Workspace


TOKEN_RE = re.compile(r"[A-Za-z0-9_]+")


def _tokenize(text: str) -> List[str]:
    return [token.lower() for token in TOKEN_RE.findall(text)]


@dataclass
class _Document:
    path: str
    title: str
    text: str
    term_frequencies: Dict[str, int]


class SearchEngine:
    def __init__(self, documents: List[_Document]) -> None:
        self.documents = documents
        self.document_count = len(documents)
        self.document_frequencies: Dict[str, int] = {}
        for document in documents:
            for token in document.term_frequencies:
                self.document_frequencies[token] = self.document_frequencies.get(token, 0) + 1

    @classmethod
    def from_workspace(cls, workspace: Workspace, snapshot: IndexSnapshot) -> "SearchEngine":
        documents: List[_Document] = []
        for artifact in snapshot.artifacts:
            if artifact.collection not in {"raw", "wiki"}:
                continue
            if artifact.kind not in {"markdown", "text", "data", "code"}:
                continue
            path = workspace.root / artifact.path
            text = path.read_text(encoding="utf-8", errors="ignore")
            tokens = _tokenize(text)
            frequencies: Dict[str, int] = {}
            for token in tokens:
                frequencies[token] = frequencies.get(token, 0) + 1
            documents.append(_Document(path=artifact.path, title=artifact.title, text=text, term_frequencies=frequencies))
        return cls(documents)

    def search(self, query: str, limit: int = 5) -> List[SearchHit]:
        query_tokens = _tokenize(query)
        if not query_tokens:
            return []

        hits: List[SearchHit] = []
        for document in self.documents:
            score = 0.0
            for token in query_tokens:
                tf = document.term_frequencies.get(token, 0)
                if not tf:
                    continue
                df = self.document_frequencies.get(token, 0)
                idf = math.log((1 + self.document_count) / (1 + df)) + 1.0
                score += (1.0 + math.log(tf)) * idf
            if score <= 0:
                continue
            hits.append(
                SearchHit(
                    path=document.path,
                    title=document.title,
                    score=round(score, 4),
                    snippet=_build_snippet(document.text, query_tokens),
                )
            )
        hits.sort(key=lambda hit: (-hit.score, hit.title.lower(), hit.path))
        return hits[:limit]


def _build_snippet(text: str, query_tokens: List[str], width: int = 200) -> str:
    lowered = text.lower()
    for token in query_tokens:
        position = lowered.find(token.lower())
        if position != -1:
            start = max(position - 60, 0)
            end = min(position + width, len(text))
            snippet = text[start:end].strip().replace("\n", " ")
            return snippet
    return text[:width].strip().replace("\n", " ")
