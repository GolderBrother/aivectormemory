import os
import sys
import numpy as np
from functools import lru_cache
from pathlib import Path
from aivectormemory.config import MODEL_NAME, MODEL_DIMENSION
from aivectormemory.log import log


class EmbeddingEngine:
    def __init__(self):
        self._session = None
        self._tokenizer = None
        self._encode_cached = lru_cache(maxsize=4096)(self._encode_impl)

    @property
    def ready(self) -> bool:
        return self._session is not None

    def load(self):
        if self.ready:
            return
        try:
            from huggingface_hub import hf_hub_download
            from tokenizers import Tokenizer
            import onnxruntime as ort

            model_dir = self._download_model(hf_hub_download)
            self._tokenizer = Tokenizer.from_file(str(model_dir / "tokenizer.json"))
            self._tokenizer.enable_padding()
            self._tokenizer.enable_truncation(max_length=512)

            model_path = model_dir / "model.onnx"
            if not model_path.exists():
                model_path = model_dir / "onnx" / "model.onnx"

            self._session = ort.InferenceSession(
                str(model_path),
                providers=["CPUExecutionProvider"]
            )
            log.info("Embedding model loaded: %s", MODEL_NAME)
        except Exception as e:
            log.error("Failed to load embedding model: %s", e)
            raise

    def _download_model(self, hf_hub_download) -> Path:
        from huggingface_hub import snapshot_download
        model_dir = Path(snapshot_download(
            MODEL_NAME,
            allow_patterns=["tokenizer.json", "tokenizer_config.json",
                           "model.onnx", "onnx/model.onnx",
                           "special_tokens_map.json", "config.json"]
        ))
        return model_dir

    def encode(self, text: str) -> list[float]:
        if not self.ready:
            self.load()
        return list(self._encode_cached(text))

    def _encode_impl(self, text: str) -> tuple[float, ...]:
        prefixed = f"query: {text}"
        encoded = self._tokenizer.encode(prefixed)

        input_ids = np.array([encoded.ids], dtype=np.int64)
        attention_mask = np.array([encoded.attention_mask], dtype=np.int64)
        token_type_ids = np.zeros_like(input_ids)

        outputs = self._session.run(
            None,
            {"input_ids": input_ids, "attention_mask": attention_mask, "token_type_ids": token_type_ids}
        )

        hidden = outputs[0]
        mask_expanded = attention_mask[:, :, np.newaxis].astype(np.float32)
        summed = (hidden * mask_expanded).sum(axis=1)
        counts = mask_expanded.sum(axis=1).clip(min=1e-9)
        pooled = summed / counts

        norm = np.linalg.norm(pooled, axis=1, keepdims=True).clip(min=1e-9)
        normalized = (pooled / norm)[0]

        return tuple(normalized.tolist())

    def encode_batch(self, texts: list[str]) -> list[list[float]]:
        return [self.encode(t) for t in texts]
