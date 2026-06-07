"""
Modul traffic_cameras (capa trusted, sense Parquet).
Flux:
    1. Llista imatges del bucket persistent sota traffic_cameras/images/raw/.
    2. Filtra per extensió (.gif, .png, .jpg, etc...).
    3. Millora cada imatge amb Pillow (brillo, nitidesa, contrast; GIF frame a frame).
    4. Puja el resultat a trusted-zone sota traffic_cameras/images/trusted/.
"""

from __future__ import annotations
import io
import os
import re
import boto3
from PIL import Image, ImageEnhance, ImageSequence

def _env(name: str, default: str) -> str:
    """Llegeix variable d'entorn, si no existeix retorna el default."""
    v = os.environ.get(name)
    return v if v is not None and v != "" else default

def _truthy(name: str, default: str = "true") -> bool:
    """Variable d'entorn com a boolea."""
    return _env(name, default).lower() in ("1", "true", "yes", "on")

def _float_env(name: str, default: str) -> float:
    """Variable d'entorn com a float."""
    try:
        return float(_env(name, default))
    except ValueError:
        return float(default)

def _int_env(name: str, default: str) -> int:
    """Variable d'entorn com a enter."""
    try:
        return int(_env(name, default))
    except ValueError:
        return int(default)

def _s3_client():
    """Client boto3 apuntant a SeaweedFS (o S3 real)."""
    endpoint = _env("S3A_ENDPOINT", _env("S3_ENDPOINT", "http://seaweedfs:8333"))
    key = _env("S3A_ACCESS_KEY", _env("AWS_ACCESS_KEY_ID", "admin"))
    secret = _env("S3A_SECRET_KEY", _env("AWS_SECRET_ACCESS_KEY", "admin"))
    return boto3.client("s3", endpoint_url=endpoint, aws_access_key_id=key, aws_secret_access_key=secret)

def _with_data_prefix(subpath: str, data_prefix: str) -> str:
    """Afegeix DATA_PREFIX al subpath i assegura barra final."""
    p = (data_prefix or "").strip("/")
    s = subpath.strip("/")
    return f"{p}/{s}/" if p else f"{s}/"

def _camera_trusted_prefix(data_prefix: str) -> str:
    """Prefix S3 de destí a trusted-zone."""
    return _with_data_prefix("traffic_cameras/images/raw", data_prefix)

def _raw_prefixes_to_scan(data_prefix: str) -> list[str]:
    """Prefixos S3 a escanejar. Prova variants de capitalització perquè S3 és case-sensitive."""
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
    """Retorna True si la clau S3 té extensió d'imatge admesa."""
    k = key.lower()
    return any(k.endswith(ext) for ext in _IMAGE_EXT)

def _relative_after_images_raw(key: str) -> str | None:
    """Extreu el path relatiu després de .../images/raw/."""
    m = re.search(r"(?i)/images/raw/(.+)$", key)
    if m:
        return m.group(1).lstrip("/")
    return None


def _normalize_trusted_out_key(trusted_prefix: str, source_key: str, rel_fallback: str) -> str:
    """Construeix la clau de sortida a trusted. A mes a mes, corregeix .pmng → .png."""
    rel = _relative_after_images_raw(source_key)
    if rel is None:
        rel = rel_fallback
    filename = rel.split("/")[-1]
    out = f"{trusted_prefix}{filename}"
    if out.lower().endswith(".pmng"):
        out = out[:-5] + ".png"
    return out


def _enhance_frame(im, brightness: float, sharpness: float, contrast: float, max_edge: int):
    """Aplica brillo, nitidesa i contrast a un frame PIL. Redimensiona si cal."""
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
    """Descodifica, millora i recodifica la imatge. Per GIFs animats processa frame a frame."""
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
    """Processa les imatges de càmeres de persistent-landing a trusted-zone."""
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
