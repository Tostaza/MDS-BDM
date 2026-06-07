"""
Utilitats de rutes URI entre landing --> trusted i sufixos relatius.

Aquest mòdul evita imports circulars: trusted_zone i trusted_pipelines comparteixen
les mateixes funcions per calcular rutes s3a:// i el sufix relatiu entre dues URIs.

Funcions:
    relative_path_under_base: extreu la part de la ruta del dataset respecte a la base del bucket.
    trusted_target_uri: compon la URI d'escriptura a la trusted-zone donat un sufix relatiu.
"""

from __future__ import annotations

def relative_path_under_base(base_uri: str, root_uri: str) -> str:
    """
    Retorna el segment de ruta de root_uri relatiu a base_uri (sense barres als extrems).
    Exemple: base s3a://bucket/prefix, root s3a://bucket/prefix/a/b/ -> a/b.
    """
    b = base_uri.rstrip("/")
    r = root_uri.rstrip("/")
    if not r.startswith(b):
        raise ValueError(f"L arrel {root_uri!r} no penja de la base {base_uri!r}")
    if r == b:
        return ""
    return r[len(b) + 1 :]


def trusted_target_uri(trusted_base_uri: str, relative: str) -> str:
    """
    Construeix la URI s3a:// de destí a trusted a partir de la base i un sufix relatiu que li passem.
    Si relative és buit, retorna la base amb barra final.
    trusted_target_uri("s3a://trusted-zone/aemet/", "daily_weather") -> "s3a://trusted-zone/aemet/daily_weather/"
    """
    t = trusted_base_uri.rstrip("/")
    if relative:
        return f"{t}/{relative.strip('/')}/"
    return f"{t}/"
