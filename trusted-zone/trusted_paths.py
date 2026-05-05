"""Helpers d'URI (landing vs trusted) compartits per trusted_zone i trusted_pipelines."""

from __future__ import annotations


def relative_path_under_base(base_uri: str, root_uri: str) -> str:
    """Part relativa de ``root_uri`` respecte a ``base_uri`` (sense barra inicial/final)."""
    b = base_uri.rstrip("/")
    r = root_uri.rstrip("/")
    if not r.startswith(b):
        raise ValueError(f"La raíz {root_uri!r} no cuelga de la base {base_uri!r}")
    if r == b:
        return ""
    return r[len(b) + 1 :]


def trusted_target_uri(trusted_base_uri: str, relative: str) -> str:
    """Construye s3a://trusted-zone/.../relativo/ a partir de la base trusted y el sufijo."""
    t = trusted_base_uri.rstrip("/")
    if relative:
        return f"{t}/{relative.strip('/')}/"
    return f"{t}/"
