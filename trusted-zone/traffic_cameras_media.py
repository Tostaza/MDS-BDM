"""
Módulo **traffic_cameras (capa trusted, sin Parquet)**.

Qué hace (flujo medalla, solo binarios):
    1. Lista objetos en el bucket ``PERSISTENT_BUCKET`` (p. ej. ``persistent-landing-zone``)
       bajo prefijos tipo ``…/traffic_cameras/images/raw/`` (respeta ``DATA_PREFIX``).
    2. Filtra extensiones de imagen (.gif, .png, .jpg, …).
    3. Descarga bytes con boto3, aplica **Pillow** (brillo, nitidez, contraste; GIF animado
       frame a frame) y opcionalmente redimensiona si ``CAMERA_MAX_EDGE`` > 0.
    4. Sube el resultado a ``TRUSTED_BUCKET`` bajo ``…/traffic_cameras/images/trusted/{timestamp}/{archivo}``.

Se invoca desde ``trusted_zone.main()`` en **Fase 0** (antes de Spark) y desde
``run_cameras_images.py`` para pruebas aisladas.

Variables de entorno (opcionales):
    CAMERA_MEDIA_ENABLED       ``false`` para desactivar (por defecto ``true``).
    CAMERA_MEDIA_RAW_PREFIX    Ruta bajo el bucket persistente sin nombre de bucket; si no
                               se define, se prueban variantes de mayúsculas en ``traffic_cameras``.
    PERSISTENT_BUCKET, TRUSTED_BUCKET, DATA_PREFIX
    S3A_* o S3_ENDPOINT + AWS_* (mismo criterio que ``ensure_bucket_exists`` en ``trusted_zone``).
    CAMERA_BRIGHTNESS / CAMERA_SHARPNESS / CAMERA_CONTRAST / CAMERA_MAX_EDGE
"""

from __future__ import annotations

import io
import os
import re


def _env(name: str, default: str) -> str:
    """Lee variable de entorno; si falta o está vacía, devuelve ``default``."""
    v = os.environ.get(name)
    return v if v is not None and v != "" else default


def _truthy(name: str, default: str = "true") -> bool:
    """Interpreta variable de entorno como booleano (1/true/yes/on)."""
    return _env(name, default).lower() in ("1", "true", "yes", "on")


def _float_env(name: str, default: str) -> float:
    """Variable de entorno como float; si no es válida, usa ``default``."""
    try:
        return float(_env(name, default))
    except ValueError:
        return float(default)


def _int_env(name: str, default: str) -> int:
    """Variable de entorno como entero; si no es válida, usa ``default``."""
    try:
        return int(_env(name, default))
    except ValueError:
        return int(default)


def _s3_client():
    """
    Connectivitat del client S3, per fer la connexió.
    """
    import boto3

    endpoint = _env("S3A_ENDPOINT", _env("S3_ENDPOINT", "http://seaweedfs:8333"))
    key = _env("S3A_ACCESS_KEY", _env("AWS_ACCESS_KEY_ID", "admin"))
    secret = _env("S3A_SECRET_KEY", _env("AWS_SECRET_ACCESS_KEY", "admin"))
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=key,
        aws_secret_access_key=secret,
    )


def _with_data_prefix(subpath: str, data_prefix: str) -> str:
    """
    Antepone DATA_PREFIX a un subpath y devuelve prefijo S3 terminado en /.
    Ej.: ("traffic_cameras/images/raw", "proyecto") → proyecto/traffic_cameras/images/raw/.
    """
    p = (data_prefix or "").strip("/")
    s = subpath.strip("/")
    return f"{p}/{s}/" if p else f"{s}/"

def _camera_trusted_prefix(data_prefix: str) -> str:
    """Prefijo S3 de destino en trusted-zone."""
    return _with_data_prefix("traffic_cameras/images/raw", data_prefix)


def _raw_prefixes_to_scan(data_prefix: str) -> list[str]:
    """
    Prefixos S3 a provar (S3 és case-sensitive). Per defecte traffic_cameras + variants típiques.
    Si CAMERA_MEDIA_RAW_PREFIX està definit, només s’usa aquest (més DATA_PREFIX si cal).
    Mètode generalització per si de cas canviessin el nom el servei d'on consumim les dades.
    """
    override = _env("CAMERA_MEDIA_RAW_PREFIX", "").strip().strip("/")
    if override:
        return [_with_data_prefix(override, data_prefix)]

    segs = [
        "traffic_cameras",
        "Traffic_cameras",
        "traffic_Cameras",
        "TRAFFIC_CAMERAS",
        "Traffic_Cameras",
    ]
    out: list[str] = []
    seen: set[str] = set()
    for seg in segs:
        p = _with_data_prefix(f"{seg}/images/raw", data_prefix)
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


_IMAGE_EXT = (
    ".gif",
    ".jpg",
    ".jpeg",
    ".png",
    ".webp",
    ".bmp",
    ".tiff",
    ".tif",
    ".pmng",  # error tipogràfic freqüent per .png
)


def _is_image_key(key: str) -> bool:
    """Devuelve True si la clave S3 termina en una extensión de imagen admitida."""
    k = key.lower()
    return any(k.endswith(ext) for ext in _IMAGE_EXT)


"""
Extreu el path després del que ve de 7raw/ --> name de la imatge, per prograpagarla a altres capes amb el mateix nom.
"""
def _relative_after_images_raw(key: str) -> str | None:
    """Extreu ``timestamp/fitxer`` després de ``.../images/raw/`` (sense importar majúscules)."""
    m = re.search(r"(?i)/images/raw/(.+)$", key)
    if m:
        return m.group(1).lstrip("/")
    return None


def _normalize_trusted_out_key(trusted_prefix: str, source_key: str, rel_fallback: str) -> str:
    """
    Construye la clave de salida en trusted (siempre bajo …/images/trusted/) + el nom de la imatge que recupea de la funcio anterior.
    Corrige extensión ``.pmng`` errónea a ``.png`` en la clave de destino.
    """
    rel = _relative_after_images_raw(source_key)
    if rel is None:
        rel = rel_fallback
    out = f"{trusted_prefix}{rel}"
    if out.lower().endswith(".pmng"):
        out = out[:-5] + ".png"
    return out


def _enhance_frame(im, brightness: float, sharpness: float, contrast: float, max_edge: int):
    """
    Aplica mejoras Pillow a un frame PIL (RGB): brillo, nitidez, contraste y resize opcional.
    Parámetros ``brightness`` / ``sharpness`` / ``contrast`` son factores (>1 intensifica).
    """
    from PIL import Image, ImageEnhance

    if im.mode not in ("RGB", "RGBA"):
        im = im.convert("RGBA") if "A" in im.mode else im.convert("RGB")
    if im.mode == "RGBA":
        base = Image.new("RGB", im.size, (255, 255, 255))
        base.paste(im, mask=im.split()[3])
        im = base
    im = ImageEnhance.Brightness(im).enhance(brightness)
    im = ImageEnhance.Sharpness(im).enhance(sharpness)
    if contrast != 1.0:
        im = ImageEnhance.Contrast(im).enhance(contrast)
    if max_edge > 0:
        w, h = im.size
        m = max(w, h)
        if m > max_edge:
            scale = max_edge / m
            im = im.resize((max(1, int(w * scale)), max(1, int(h * scale))), Image.Resampling.LANCZOS)
    return im


def _bytes_to_enhanced_image(raw: bytes, brightness: float, sharpness: float, contrast: float, max_edge: int) -> bytes:
    """
    Decodifica bytes de imagen, mejora y vuelve a codificar a bytes (GIF/JPEG/PNG/WebP según formato).
    GIF con varios frames: procesa cada frame y reensambla el GIF.
    """
    from PIL import Image, ImageSequence

    src = io.BytesIO(raw)
    im = Image.open(src)
    fmt = (im.format or "PNG").upper()

    if fmt == "GIF" and getattr(im, "n_frames", 1) > 1:
        frames_out = []
        durations = []
        for frame in ImageSequence.Iterator(im):
            durations.append(im.info.get("duration", 100))
            frames_out.append(_enhance_frame(frame, brightness, sharpness, contrast, max_edge))
        out = io.BytesIO()
        # AQUI ANIREM AFEGINT LA SEQÜENCIA DE FRAMES.
        frames_out[0].save(
            out,
            format="GIF",
            save_all=True,
            append_images=frames_out[1:],
            duration=durations[: len(frames_out)] or None,
            loop=im.info.get("loop", 0),
            optimize=False,
        )
        return out.getvalue()

    # SERIE LA PART DEL ELSE, QUAN NO ES UN GIF I PER TANT TRACTEM LA IMATGE PER SI SOLA.
    single = _enhance_frame(im, brightness, sharpness, contrast, max_edge)
    out = io.BytesIO()
    if fmt in ("JPEG", "JPG"):
        single.save(out, format="JPEG", quality=92, optimize=True)
    elif fmt == "GIF":
        single.save(out, format="GIF", save_all=False, optimize=False)
    elif fmt == "WEBP":
        single.save(out, format="WEBP", quality=88)
    else:
        single.save(out, format="PNG", optimize=True)
    return out.getvalue()


def process_traffic_cameras_persistent_to_trusted() -> int:

    if not _truthy("CAMERA_MEDIA_ENABLED", "true"):
        print("traffic_cameras (imatges): CAMERA_MEDIA_ENABLED=false, s'omet.")
        return 0

    pillow_ok = False
    try:
        from PIL import Image  # noqa: F401
        pillow_ok = True
    except ImportError:
        print("[traffic_cameras] Pillow no disponible, es copiaran les imatges sense millora.")

    persistent_bucket = _env("PERSISTENT_BUCKET", "persistent-landing-zone")
    trusted_bucket = _env("TRUSTED_BUCKET", "trusted-zone")
    data_prefix = _env("DATA_PREFIX", "").strip("/")

    trusted_prefix = _camera_trusted_prefix(data_prefix)
    raw_prefixes = _raw_prefixes_to_scan(data_prefix)

    brightness = _float_env("CAMERA_BRIGHTNESS", "1.06")
    sharpness = _float_env("CAMERA_SHARPNESS", "1.15")
    contrast = _float_env("CAMERA_CONTRAST", "1.05")
    max_edge = _int_env("CAMERA_MAX_EDGE", "0")

    s3 = _s3_client()

    keys: list[str] = []
    seen_keys: set[str] = set()
    for raw_prefix in raw_prefixes:
        token = None
        while True:
            kw: dict = {"Bucket": persistent_bucket, "Prefix": raw_prefix}
            if token:
                kw["ContinuationToken"] = token
            resp = s3.list_objects_v2(**kw)
            n_here = len(resp.get("Contents", []) or [])
            if n_here:
                print(f"[traffic_cameras]   list prefix {raw_prefix!r} → {n_here} objeto(s) (página)")
            for obj in resp.get("Contents", []) or []:
                k = obj.get("Key") or ""
                if not _is_image_key(k):
                    continue
                if k not in seen_keys:
                    seen_keys.add(k)
                    keys.append(k)
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")

    if not keys:
        tried = ", ".join(f"s3://{persistent_bucket}/{p}" for p in raw_prefixes[:5])
        more = f" (+{len(raw_prefixes) - 5} variants)" if len(raw_prefixes) > 5 else ""
        print(
            "traffic_cameras (imatges): cap fitxer d'imatge trobat. "
            f"Prefixes provats (mostra): {tried}{more}. "
            "Defineix CAMERA_MEDIA_RAW_PREFIX amb el path exacte de les teves claus S3 si cal."
        )
        return 0

    n_ok = 0
    for key in sorted(keys):
        rel = _relative_after_images_raw(key)
        if rel is None:
            rel = key.split("/")[-1]
        out_key = _normalize_trusted_out_key(trusted_prefix, key, rel)
        try:
            body = s3.get_object(Bucket=persistent_bucket, Key=key)["Body"].read()
            if pillow_ok:
                try:
                    enhanced = _bytes_to_enhanced_image(body, brightness, sharpness, contrast, max_edge)
                except Exception as ex:
                    print(f"  traffic_cameras: millora fallida per {key!r} ({ex}); es copia el binari original.")
                    enhanced = body
            else:
                enhanced = body
            s3.put_object(Bucket=trusted_bucket, Key=out_key, Body=enhanced)
            n_ok += 1
            print(f"  traffic_cameras: {key} → s3://{trusted_bucket}/{out_key}")
        except Exception as ex:
            print(f"  traffic_cameras: error {key!r}: {ex}")

    print(f"traffic_cameras (imatges): {n_ok}/{len(keys)} fitxers escrits a trusted.")
    return n_ok
