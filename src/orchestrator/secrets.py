from __future__ import annotations

import os
from pathlib import Path
from typing import Protocol


class SecretStore(Protocol):
    def put(self, name: str, value: str) -> None: ...
    def get(self, name: str) -> str: ...


class EncryptedSecretStore:
    """Fernet-backed local secret store; key material must come from a KMS/env in deployment."""

    def __init__(self, path: str | Path, key: bytes | None = None):
        try:
            from cryptography.fernet import Fernet
        except ImportError as exc:  # pragma: no cover - exercised only without secure extra
            raise RuntimeError("install the 'secure' extra to use EncryptedSecretStore") from exc
        self._fernet = Fernet(key or os.environ.get("ORCHESTRATOR_SECRET_KEY", "").encode())
        self.path = Path(path)

    @staticmethod
    def generate_key() -> bytes:
        from cryptography.fernet import Fernet
        return Fernet.generate_key()

    def put(self, name: str, value: str) -> None:
        values = self._read()
        values[name] = self._fernet.encrypt(value.encode()).decode()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(self.path.suffix + ".tmp")
        import json
        temporary.write_text(json.dumps(values, sort_keys=True), encoding="utf-8")
        temporary.replace(self.path)

    def get(self, name: str) -> str:
        import json
        try:
            return self._fernet.decrypt(self._read()[name].encode()).decode()
        except KeyError as exc:
            raise KeyError(f"secret not found: {name}") from exc

    def _read(self) -> dict[str, str]:
        import json
        if not self.path.exists():
            return {}
        return json.loads(self.path.read_text(encoding="utf-8"))
