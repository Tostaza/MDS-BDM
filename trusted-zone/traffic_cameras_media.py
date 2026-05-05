"""
Imatges de càmeres de trànsit (GIF/JPG/…): persistent-landing → trusted-zone **sense Parquet**.

Es llegeixen bytes des de ``{DATA_PREFIX}/traffic_cameras/images/raw/`` (sense ``DATA_PREFIX``,
el camí coincideix amb la bronze SCT; si no, cal el mateix prefix a les claus S3).
S'apliquen millores lleus (lluminositat, nitidesa, contrast, redimensió opcional) amb Pillow
i es graven a ``…/traffic_cameras/images/trusted/`` mantenint ``{timestamp}/{fitxer}``.

Variables d'entorn (opcionals):
    CAMERA_MEDIA_ENABLED       "false" per ometre tot el bloc (default true)
    CAMERA_MEDIA_RAW_PREFIX    prefix complet sota el bucket persistent (sense bucket);
                               per defecte ``{DATA_PREFIX}/traffic_cameras/images/raw/``.
                               S3 és **case-sensitive**: si les claus usen ``Traffic_Cameras``, passa
                               aquest valor exacte o deixa que es provin variants automàticament.
    CAMERA_BRIGHTNESS          factor Pillow ImageEnhance.Brightness (default 1.06)
    CAMERA_SHARPNESS           factor ImageEnhance.Sharpness (default 1.15)
    CAMERA_CONTRAST            factor ImageEnhance.Contrast (default 1.05; 1.0 = neutral)
    CAMERA_MAX_EDGE            píxel màxim de la vora llarga; 0 = sense redimensionar (default 0)
"""

from __future__ import annotations

import io
import os
import re
# -----------------------------------------------------------------------------
# Només s'executa si Pillow està instal·lat (p. ex. imatge Spark Docker després de pip install Pillow).
# -----------------------------------------------------------------------------


def _env(name: str, default: str) -> str:
    v = os.environ.get(name)
    return v if v is not None and v != "" else default


def _truthy(name: str, default: str = "true") -> bool:
    return _env(name, default).lower() in ("1", "true", "yes", "on")


def _float_env(name: str, default: str) -> float:
    try:
        return float(_env(name, default))
    except ValueError:
        return float(default)


def _int_env(name: str, default: str) -> int:
    try:
        return int(_env(name, default))
    except ValueError:
        return int(default)


def _s3_client():
    """
    Mateixa configuració que ``ensure_bucket_exists`` a ``trusted_zone.py`` (boto per defecte),
    més ``S3_ENDPOINT`` / ``AWS_*`` com a fallback per si només exportes això al ``docker exec``.
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
    """Prefix comú DATA_PREFIX (alineat amb landing/trusted URI)."""
    p = (data_prefix or "").strip("/")
    s = subpath.strip("/")
    return f"{p}/{s}/" if p else f"{s}/"


def _camera_raw_prefix(data_prefix: str) -> str:
    return _with_data_prefix("traffic_cameras/images/raw", data_prefix)


def _camera_trusted_prefix(data_prefix: str) -> str:
    return _with_data_prefix("traffic_cameras/images/trusted", data_prefix)


def _raw_prefixes_to_scan(data_prefix: str) -> list[str]:
    """
    Prefixos S3 a provar (S3 és case-sensitive). Per defecte ``traffic_cameras`` + variants típiques.
    Si ``CAMERA_MEDIA_RAW_PREFIX`` està definit, només s’usa aquest (més ``DATA_PREFIX`` si cal).
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
    k = key.lower()
    return any(k.endswith(ext) for ext in _IMAGE_EXT)


def _relative_after_images_raw(key: str) -> str | None:
    """Extreu ``timestamp/fitxer`` després de ``.../images/raw/`` (sense importar majúscules)."""
    m = re.search(r"(?i)/images/raw/(.+)$", key)
    if m:
        return m.group(1).lstrip("/")
    return None


def _normalize_trusted_out_key(trusted_prefix: str, source_key: str, rel_fallback: str) -> str:
    """Sempre escriu sota el prefix canònic ``traffic_cameras/images/trusted/``."""
    rel = _relative_after_images_raw(source_key)
    if rel is None:
        rel = rel_fallback
    out = f"{trusted_prefix}{rel}"
    if out.lower().endswith(".pmng"):
        out = out[:-5] + ".png"
    return out


def _enhance_frame(im, brightness: float, sharpness: float, contrast: float, max_edge: int):
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
    """
    Llista objectes sota ``traffic_cameras/images/raw/`` al bucket persistent,
    millora cada imatge i puja el resultat a ``traffic_cameras/images/trusted/`` al bucket trusted.

    Retorna el nombre de fitxers processats (0 si desactivat o sense Pillow / sense claus).
    """
    if not _truthy("CAMERA_MEDIA_ENABLED", "true"):
        print("traffic_cameras (imatges): CAMERA_MEDIA_ENABLED=false, s'omet.")
        return 0

    try:
        from PIL import Image  # noqa: F401

        print(f"[traffic_cameras] Pillow OK (versió importable)")
    except ImportError:
        print(
            "traffic_cameras (imatges): Pillow NO instal·lat. "
            "Al Docker Spark: `pip install Pillow` o reconstrueix la imatge (Dockerfile amb Pillow)."
        )
        return 0

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
    ep = _env("S3A_ENDPOINT", _env("S3_ENDPOINT", "http://seaweedfs:8333"))
    print(
        f"[traffic_cameras] boto3 endpoint={ep!r} "
        f"persistent_bucket={persistent_bucket!r} trusted_bucket={trusted_bucket!r}"
    )
    print(f"[traffic_cameras] prefixes a listar ({len(raw_prefixes)}): {raw_prefixes!r}")
    try:
        probe = s3.list_objects_v2(Bucket=persistent_bucket, MaxKeys=5)
        sample = [o.get("Key") for o in (probe.get("Contents") or [])[:5]]
        print(f"[traffic_cameras] prova list_bucket OK (mostra fins 5 claus): {sample}")
    except Exception as ex:
        print(f"[traffic_cameras] ERROR: no es pot listar el bucket persistent {persistent_bucket!r}: {ex}")
        return 0

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
            try:
                enhanced = _bytes_to_enhanced_image(body, brightness, sharpness, contrast, max_edge)
            except Exception as ex:
                print(f"  traffic_cameras: millora fallida per {key!r} ({ex}); es copia el binari original.")
                enhanced = body
            s3.put_object(Bucket=trusted_bucket, Key=out_key, Body=enhanced)
            n_ok += 1
            print(f"  traffic_cameras: {key} → s3://{trusted_bucket}/{out_key}")
        except Exception as ex:
            print(f"  traffic_cameras: error {key!r}: {ex}")

    print(f"traffic_cameras (imatges): {n_ok}/{len(keys)} fitxers escrits a trusted.")
    return n_ok
