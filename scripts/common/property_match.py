"""Fuzzy resolver for property names across sources.

Doc Único uses Building text (e.g. "ATLANTIC LODGE").
Rental Ready uses display_name with 'TN -' prefix (e.g. "T2 - Divine Waves Duplex").
Guesty uses a more compact form (e.g. "T2-DivineWavesDuplex").

Strategy: build a dict of normalized variants → property_uuid and look up by best
normalized match. We aggressively normalize: lowercase, remove diacritics, drop
non-alphanumerics. Sprint 2 will replace this with explicit alias columns.
"""
from __future__ import annotations

import re
from typing import Dict, Iterable, Optional

from unidecode import unidecode


def _norm(s: str) -> str:
    if not s:
        return ""
    return re.sub(r"[^a-z0-9]+", "", unidecode(s).lower())


class PropertyResolver:
    def __init__(self):
        self._by_normalized: Dict[str, str] = {}

    @classmethod
    def from_db(cls, conn, entity_id: str) -> "PropertyResolver":
        r = cls()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, canonical_name, display_name, building,
                       doc_unico_id, rental_ready_id, guesty_id
                FROM properties WHERE entity_id = %s
                """,
                (entity_id,),
            )
            for row in cur.fetchall():
                pid = str(row[0])
                for variant in row[1:]:
                    if variant:
                        r._add(str(variant), pid)
        return r

    def _add(self, name: str, pid: str) -> None:
        n = _norm(name)
        if n:
            self._by_normalized.setdefault(n, pid)

    def add_alias(self, alias: str, pid: str) -> None:
        self._add(alias, pid)

    def resolve(self, name: str) -> Optional[str]:
        if not name:
            return None
        n = _norm(name)
        if n in self._by_normalized:
            return self._by_normalized[n]
        # Try suffix/prefix substring match
        for k, pid in self._by_normalized.items():
            if n in k or k in n:
                return pid
        return None

    def all_known(self) -> Iterable[str]:
        return self._by_normalized.keys()
