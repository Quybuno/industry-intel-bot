"""Stage 2b — embedding similarity filter against interest profile."""
from __future__ import annotations

import logging
import math
import re
from collections import Counter
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

MAX_CHARS = 2048  # ~512 tokens rough limit


def _tokenize(text: str) -> list[str]:
    return re.findall(r'[a-z0-9]+', text.lower())


def _bow_cosine(a: str, b: str) -> float:
    """Lightweight fallback when sentence-transformers is unavailable."""
    ca, cb = Counter(_tokenize(a)), Counter(_tokenize(b))
    if not ca or not cb:
        return 0.0
    keys = set(ca) | set(cb)
    dot = sum(ca[k] * cb[k] for k in keys)
    na = math.sqrt(sum(v * v for v in ca.values()))
    nb = math.sqrt(sum(v * v for v in cb.values()))
    return dot / (na * nb) if na and nb else 0.0


class EmbeddingFilter:
    def __init__(
        self,
        profile_path: str,
        *,
        threshold: float = 0.35,
        fallback_threshold: float = 0.15,
        model_name: str = 'BAAI/bge-small-en-v1.5',
    ):
        self.threshold = threshold
        self.fallback_threshold = fallback_threshold
        self.model_name = model_name
        self.profile_text = Path(profile_path).read_text(encoding='utf-8').strip()
        self._model = None
        self._profile_embedding = None
        self._mode: Optional[str] = None  # 'transformer' | 'bow'

    def _init_model(self) -> None:
        if self._mode is not None:
            return
        try:
            from sentence_transformers import SentenceTransformer
            import numpy as np

            self._model = SentenceTransformer(self.model_name)
            self._profile_embedding = self._model.encode(
                self.profile_text, normalize_embeddings=True
            )
            self._np = np
            self._mode = 'transformer'
            logger.info('Embedding filter using %s', self.model_name)
        except ImportError:
            self._mode = 'bow'
            logger.warning(
                'sentence-transformers not installed; using bag-of-words fallback '
                '(pip install sentence-transformers for bge-small-en-v1.5)'
            )

    def _truncate(self, text: str) -> str:
        return text[:MAX_CHARS] if text else ''

    def similarity(self, text: str) -> float:
        self._init_model()
        text = self._truncate(text)

        if self._mode == 'transformer':
            vec = self._model.encode(text, normalize_embeddings=True)
            return float(self._np.dot(vec, self._profile_embedding))

        return _bow_cosine(text, self.profile_text)

    def passes(self, text: str) -> tuple[bool, float, str]:
        """Returns (passed, score, mode)."""
        score = self.similarity(text)
        active_threshold = self.threshold if self._mode == 'transformer' else self.fallback_threshold
        return score >= active_threshold, score, self._mode or 'bow'
