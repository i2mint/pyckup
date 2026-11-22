"""Offline tests for pyckup: protocol dispatch, local files, and the OWID acquirer with a fake downloader."""

import io
import zipfile
from functools import partial
from pathlib import Path

import pytest

from pyckup import grab
from pyckup import base
from pyckup.sources import owid


def test_grab_local_file_and_folder(tmp_path):
    f = tmp_path / "a.bin"
    f.write_bytes(b"xyz")
    assert grab(str(f)) == b"xyz"
    assert grab("file://" + str(f)) == b"xyz"
    folder = grab(str(tmp_path))
    assert "a.bin" in folder and folder["a.bin"] == b"xyz"


def test_grab_custom_and_unknown_protocol():
    g = partial(grab, protocols={"foo": str.upper})
    assert g("foo://bar") == "FOO://BAR"
    with pytest.raises(KeyError, match="Unrecognized protocol: nope"):
        g("nope://x")
    assert grab("x://y", protocols=lambda: {"x": lambda k: k[::-1]}) == "y//:x"


def test_url_2_bytes_reads_in_chunks(tmp_path):
    f = tmp_path / "big.bin"
    f.write_bytes(b"0123456789" * 300)
    assert base.url_2_bytes(f.as_uri(), chk_size=7) == b"0123456789" * 300


def test_slug_to_url_and_filename(tmp_path):
    url, path = owid.slug_to_url_and_filename("co2", "csv", rootdir=str(tmp_path))
    assert url == "https://ourworldindata.org/grapher/co2.csv"
    assert path == str(tmp_path / "co2.csv")
    assert owid.slug_to_url_and_filename("co2", "zip", str(tmp_path))[0].endswith(".zip")
    assert owid.slug_to_url_and_filename("co2", "html", str(tmp_path))[0].endswith("/co2")
    assert owid.slug_to_url_and_filename("co2", "json", str(tmp_path))[0].endswith(
        "co2.metadata.json"
    )
    with pytest.raises(ValueError, match="Unknown kind"):
        owid.slug_to_url_and_filename("co2", "pdf", str(tmp_path))


@pytest.fixture
def rooted(tmp_path, monkeypatch):
    orig = owid.slug_to_url_and_filename
    monkeypatch.setattr(
        owid, "slug_to_url_and_filename", lambda slug, kind="json": orig(slug, kind, str(tmp_path))
    )
    return tmp_path


def _fake_downloader(contents, fail_on=()):
    def download(url, *, filepath, overwrite, return_func):
        for marker, exc in fail_on:
            if url.endswith(marker):
                raise exc
        Path(filepath).write_bytes(contents[url.rsplit(".", 1)[-1] if url.endswith(".csv") else "json"])
        return filepath

    return download


def test_acquire_downloads_csv_and_json(rooted, monkeypatch):
    monkeypatch.setattr(
        owid, "url_to_file_download",
        _fake_downloader({"csv": b"a,b\n1,2\n", "json": b'{"chart": {"title": "T"}}'}),
    )
    json_file, csv_file = owid.acquire_owid_data("co2")
    assert Path(csv_file).read_bytes() == b"a,b\n1,2\n"
    assert Path(json_file).read_text().startswith('{"chart"')


def test_acquire_uses_existing_files_without_downloading(rooted, monkeypatch):
    (rooted / "co2.csv").write_text("x")
    (rooted / "co2.metadata.json").write_text("{}")

    def boom(*a, **k):
        raise AssertionError("should not download")

    monkeypatch.setattr(owid, "url_to_file_download", boom)
    json_file, csv_file = owid.acquire_owid_data("co2")
    assert Path(csv_file).name == "co2.csv" and Path(json_file).name == "co2.metadata.json"


def test_acquire_non_redistributable_gives_none(rooted, monkeypatch, capsys):
    err = RuntimeError("403 non-redistributable")
    monkeypatch.setattr(
        owid, "url_to_file_download",
        _fake_downloader({}, fail_on=[(".csv", err), (".json", err)]),
    )
    assert owid.acquire_owid_data("co2") == (None, None)
    assert "non-redistributable" in capsys.readouterr().out


def test_acquire_forbidden_falls_back_to_zip(rooted, monkeypatch):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("co2.csv", "a\n1\n")
        z.writestr("co2.metadata.json", "{}")
    zbytes = buf.getvalue()

    def download(url, *, filepath, overwrite, return_func):
        if url.endswith(".zip"):
            Path(filepath).write_bytes(zbytes)
            return filepath
        raise RuntimeError("403 Forbidden")

    monkeypatch.setattr(owid, "url_to_file_download", download)
    json_file, csv_file = owid.acquire_owid_data("co2")
    assert Path(csv_file).read_text() == "a\n1\n"
    assert Path(json_file).read_text() == "{}"
