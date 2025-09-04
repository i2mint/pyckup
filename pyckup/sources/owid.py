"""
OWID data source.

You can see a list of datas here: https://ourworldindata.org/data

TODO: Write a function that lists all the available datas?
TODO: Write a dol interface to metadata (json) and data (csv or zip), like with haggle (kaggle interface), etc.

See: https://docs.owid.io/projects/etl/api/chart-api/

"""

import os
import json
from pathlib import Path
from typing import Optional, Tuple, List
import zipfile
from aix.util import model_info_dir

# graze helpers
from graze.base import url_to_file_download, return_filepath

from pyckup.util import downloads_dir


DFLT_REFRESH = False

owid_downloads_dir = os.path.join(downloads_dir, "owid")
Path(owid_downloads_dir).mkdir(parents=True, exist_ok=True)
owid_downloads_dir_path = str(owid_downloads_dir)


def slug_to_url_and_filename(slug, kind='json', rootdir=owid_downloads_dir_path):

    base = f"https://ourworldindata.org/grapher/{slug}"

    if kind == 'json':
        url = f"{base}.metadata.json"
        path = os.path.join(rootdir, f"{slug}.metadata.json")
    elif kind == 'csv':
        url = f"{base}.csv"
        path = os.path.join(rootdir, f"{slug}.csv")
    elif kind == 'zip':
        url = f"{base}.zip"
        path = os.path.join(rootdir, f"{slug}.zip")
    elif kind == 'html':
        url = f"{base}"
        path = os.path.join(rootdir, f"{slug}.html")
    else:
        raise ValueError(
            f"Unknown kind: {kind} (should be 'json', 'csv', 'zip', or 'html')"
        )

    return url, path


def acquire_owid_data(
    slug: str, *, refresh: Optional[bool] = DFLT_REFRESH
) -> List[Tuple[str, Optional[Path], Optional[Path]]]:
    """Download OWID CSV + JSON metadata for each chart slug.

    Returns list of tuples: (json_path_or_None, csv_path_or_None)
    """

    attempted_zip = set()  # track slug ZIP fallback attempts to avoid duplicates

    def _is_forbidden_error(err: Exception) -> bool:
        msg = str(err)
        return "403" in msg or "Forbidden" in msg

    def _is_non_redistributable(err: Exception) -> bool:
        # Detect server message that explicitly forbids redistribution
        msg = str(err).lower()
        return "non-redistributable" in msg or "not allowed to re-share" in msg

    def _download_from_zip(
        slug: str, csv_path: Path, json_path: Path
    ) -> Tuple[Optional[Path], Optional[Path]]:
        zip_url, tmp_zip_path = slug_to_url_and_filename(slug, kind='zip')
        tmp_zip_path = Path(tmp_zip_path)

        # Avoid re-attempting ZIP if we already tried once
        if slug in attempted_zip and not refresh:
            return None, None

        attempted_zip.add(slug)

        try:
            zip_ret = url_to_file_download(
                zip_url,
                filepath=str(tmp_zip_path),
                overwrite=bool(refresh),
                return_func=return_filepath,
            )
            if not zip_ret:
                return None, None
            zip_file_path = Path(zip_ret)
            if not zip_file_path.exists():
                return None, None
        except Exception as e:
            # If server forbids redistribution, bail early and don't spam retries
            if _is_non_redistributable(e):
                print(
                    f"OWID ZIP fallback: data for '{slug}' is non-redistributable; skipping ZIP fallback."
                )
                return None, None
            print(f"OWID ZIP fallback for '{slug}' failed: {e}")
            return None, None

        csv_ok, json_ok = None, None
        try:
            with zipfile.ZipFile(zip_file_path, 'r') as zf:
                csv_member = f"{slug}.csv"
                meta_member = f"{slug}.metadata.json"

                try:
                    with zf.open(csv_member) as fsrc, open(csv_path, "wb") as fdst:
                        fdst.write(fsrc.read())
                    csv_ok = csv_path
                    print(
                        f"✓ OWID CSV (from ZIP): {csv_path.name} ({csv_path.stat().st_size:,} bytes)"
                    )
                except Exception as e:
                    print(f"Could not extract CSV from ZIP for '{slug}': {e}")

                try:
                    with zf.open(meta_member) as fsrc, open(json_path, "wb") as fdst:
                        fdst.write(fsrc.read())
                    json_ok = json_path
                    print(
                        f"✓ OWID JSON (from ZIP): {json_path.name} ({json_path.stat().st_size:,} bytes)"
                    )
                except Exception as e:
                    print(f"Could not extract metadata JSON from ZIP for '{slug}': {e}")
        except Exception as e:
            print(f"OWID ZIP for '{slug}' could not be read: {e}")
        finally:
            try:
                if zip_file_path.exists():
                    zip_file_path.unlink()
            except Exception:
                pass

        return csv_ok, json_ok

    json_url, json_path = slug_to_url_and_filename(slug, kind='json')
    csv_url, csv_path = slug_to_url_and_filename(slug, kind='csv')

    # ---- CSV ----
    csv_file = None
    if Path(csv_path).exists() and not refresh:
        csv_file = csv_path
    else:
        # Attempt direct CSV
        print(f"Acquiring OWID CSV for '{slug}' -> {csv_url}")
        try:
            csv_ret = url_to_file_download(
                csv_url,
                filepath=str(csv_path),
                overwrite=bool(refresh),
                return_func=return_filepath,
            )
            csv_file = Path(csv_ret) if csv_ret else None
            if csv_file and csv_file.exists():
                print(
                    f"✓ OWID CSV: {csv_file.name} ({csv_file.stat().st_size:,} bytes)"
                )
            else:
                csv_file = None
        except Exception as e:
            if _is_non_redistributable(e):
                print(
                    f"OWID CSV for '{slug}' is non-redistributable; not available for download."
                )
                csv_file = None
            elif _is_forbidden_error(e):
                # Only try ZIP fallback if we haven't already attempted it for this slug
                if slug not in attempted_zip:
                    print(
                        f"CSV download forbidden for '{slug}', attempting ZIP fallback…"
                    )
                    csv_file, json_file_from_zip = _download_from_zip(
                        slug, csv_path, json_path
                    )
                else:
                    csv_file = None
                    json_file_from_zip = None
            else:
                print(f"OWID CSV for '{slug}' failed: {e}")
                csv_file = None
                json_file_from_zip = None

    # ---- JSON metadata ----
    json_file: Optional[Path] = None
    if Path(json_path).exists() and not refresh:
        json_file = json_path
    else:
        if "json_file_from_zip" in locals() and json_file_from_zip:
            json_file = json_file_from_zip
        else:
            try:
                print(f"Acquiring OWID JSON for '{slug}' -> {json_url}")
                json_ret = url_to_file_download(
                    json_url,
                    filepath=str(json_path),
                    overwrite=bool(refresh),
                    return_func=return_filepath,
                )
                json_file = Path(json_ret) if json_ret else None
                if json_file and json_file.exists():
                    try:
                        meta = json.loads(json_file.read_text(encoding="utf-8"))
                        title = meta.get("chart", {}).get("title", slug)
                        print(
                            f"✓ OWID JSON: {json_file.name} ({json_file.stat().st_size:,} bytes)"
                        )
                        print(f"  • Chart title: {title}")
                    except Exception:
                        pass
                else:
                    json_file = None
            except Exception as e:
                if _is_non_redistributable(e):
                    print(
                        f"OWID JSON for '{slug}' appears non-redistributable; skipping."
                    )
                    json_file = None
                elif _is_forbidden_error(e) and slug not in attempted_zip:
                    print(
                        f"Metadata download forbidden for '{slug}', attempting ZIP fallback…"
                    )
                    _, json_file_zip = _download_from_zip(slug, csv_path, json_path)
                    json_file = json_file_zip
                else:
                    print(f"OWID JSON for '{slug}' failed: {e}")
                    json_file = None

    return json_file, csv_file
