"""
Utilidades de **rutas URI** entre landing, trusted y sufijos relativos.

Este módulo evita imports circulares: ``trusted_zone`` y ``trusted_pipelines`` comparten
las mismas funciones para calcular rutas ``s3a://`` y el sufijo relativo entre dos URIs.

Funciones:
    ``relative_path_under_base``: parte de una raíz de dataset respecto a la base del bucket.
    ``trusted_target_uri``: compone la URI de escritura en trusted-zone dado un sufijo relativo.
"""

from __future__ import annotations


def relative_path_under_base(base_uri: str, root_uri: str) -> str:
    """
    Devuelve el segmento de ruta de ``root_uri`` relativo a ``base_uri`` (sin barras extremas).

    Ej.: base ``s3a://bucket/prefix``, root ``s3a://bucket/prefix/a/b/`` → ``a/b``.
    """
    b = base_uri.rstrip("/")
    r = root_uri.rstrip("/")
    if not r.startswith(b):
        raise ValueError(f"La raíz {root_uri!r} no cuelga de la base {base_uri!r}")
    if r == b:
        return ""
    return r[len(b) + 1 :]


def trusted_target_uri(trusted_base_uri: str, relative: str) -> str:
    """
    Construye la URI ``s3a://`` de destino en trusted a partir de la base y un sufijo relativo.

    Si ``relative`` está vacío, devuelve la base con barra final.
    """
    t = trusted_base_uri.rstrip("/")
    if relative:
        return f"{t}/{relative.strip('/')}/"
    return f"{t}/"
