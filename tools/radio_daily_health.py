#!/usr/bin/env python3
"""Auditoria diaria, solo lectura, de streams de Radio Mexico de Siempre."""
import argparse
import concurrent.futures
import csv
import datetime as dt
import difflib
import http.client
import json
import pathlib
import re
import shutil
import statistics
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter

CATALOG = "https://fernandodevsr.github.io/stations-live.json"
RADIO_BROWSER = "https://de1.api.radio-browser.info/json/stations/search"
UA = "FAAppStudio-RadioMX-Monitor/1.0"
TIMEOUT = 9
SAMPLE = 16384


def download_json(url, timeout=15):
    request = urllib.request.Request(url, headers={
        "User-Agent": UA, "Accept": "application/json", "Cache-Control": "no-cache"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read(1000000).decode("utf-8-sig"))


def audio_signature(data):
    head = data[:180].lstrip().lower()
    if head.startswith((b"<html", b"<!doctype", b"<?xml", b"{", b"[")):
        return "NOT_AUDIO"
    if data.startswith((b"ID3", b"OggS", b"fLaC", b"RIFF")):
        return "AUDIO_MAGIC"
    if len(data) > 12 and data[4:8] == b"ftyp":
        return "MP4"
    if b"#EXTM3U" in data[:40]:
        return "M3U"
    if data.startswith(b"[playlist]"):
        return "PLS"
    if len(data) > 189 and data[0] == 71 and data[188] == 71:
        return "MPEG_TS"
    frames = 0
    for i in range(max(0, min(len(data)-2, 8192))):
        if data[i] == 255 and (data[i+1] & 224) == 224 and (data[i+2] & 240) != 240:
            frames += 1
            if frames >= 3:
                return "MP3_AAC_FRAMES"
    return "UNKNOWN"


def decode_codec(data):
    if len(data) < 2048 or shutil.which("ffprobe") is None:
        return ""
    try:
        result = subprocess.run(["ffprobe", "-v", "error", "-show_entries",
            "stream=codec_type,codec_name", "-of", "json", "-i", "pipe:0"],
            input=data, capture_output=True, timeout=4)
        parsed = json.loads(result.stdout or b"{}")
        for entry in parsed.get("streams", []):
            if entry.get("codec_type") == "audio":
                return entry.get("codec_name") or "audio"
    except (OSError, ValueError, subprocess.TimeoutExpired):
        pass
    return ""


def probe(url, depth=0):
    start = time.monotonic()
    out = {"ok": False, "likely": False, "header_ms": None, "first_audio_ms": None,
           "bytes": 0, "reason": "", "codec": "", "http_status": None}
    try:
        request = urllib.request.Request(url, headers={
            "User-Agent": UA, "Accept": "*/*", "Icy-MetaData": "0",
            "Connection": "close"})
        with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
            out["header_ms"] = round((time.monotonic()-start)*1000)
            out["http_status"] = getattr(response, "status", 200)
            mime = (response.headers.get("Content-Type") or "").lower()
            first = response.read(1)
            out["first_audio_ms"] = round((time.monotonic()-start)*1000) if first else None
            sample = first + response.read(SAMPLE-1) if first else b""
            out["bytes"] = len(sample)
            kind = audio_signature(sample)
            if kind in ("M3U", "PLS") and depth < 2:
                lines = sample.decode("utf-8", "replace").splitlines()
                entries = ([line.strip() for line in lines
                    if line.strip() and not line.lstrip().startswith("#")]
                    if kind == "M3U" else
                    [line.partition("=")[2].strip() for line in lines
                     if re.match(r"File\d+=", line, re.I)])
                if entries:
                    child = probe(urllib.parse.urljoin(response.geturl(), entries[0]), depth+1)
                    out["ok"] = child["ok"]
                    out["likely"] = child["likely"]
                    out["codec"] = child["codec"]
                    out["reason"] = "PLAYLIST_" + child["reason"]
                    if child["first_audio_ms"] is not None:
                        out["first_audio_ms"] = round((time.monotonic()-start)*1000)
                    return out
            out["codec"] = decode_codec(sample)
            out["ok"] = len(sample) >= 2048 and (
                bool(out["codec"]) or kind in ("AUDIO_MAGIC", "MP4", "MPEG_TS", "MP3_AAC_FRAMES"))
            out["likely"] = not out["ok"] and len(sample) >= 4096 and (
                mime.startswith("audio/") or "mpeg" in mime or "aac" in mime)
            out["reason"] = "AUDIO_CONFIRMED" if out["ok"] else (
                "AUDIO_MIME_UNVERIFIED" if out["likely"] else kind)
    except urllib.error.HTTPError as error:
        out["http_status"] = error.code
        out["reason"] = "HTTP_" + str(error.code)
    except http.client.BadStatusLine as error:
        # Algunas radios utilizan respuestas ICY en vez de HTTP.
        # Un ICY 401 NO es audio: es un rechazo del servidor.
        raw = str(error).strip()
        match = re.search(r"^ICY\s+(\d{3})", raw, re.I)
        if match:
            out["http_status"] = int(match.group(1))
            out["reason"] = "ICY_" + match.group(1)
        else:
            out["reason"] = "INVALID_HTTP_STATUS"
        out["error"] = raw[:160]
    except Exception as error:
        out["reason"] = type(error).__name__
        out["error"] = str(error)[:160]
    out["elapsed_ms"] = round((time.monotonic()-start)*1000)
    return out


def check_station(station):
    urls = list(dict.fromkeys(station.get("streams") or []))
    streams = []
    for index, url in enumerate(urls, 1):
        attempts = [probe(url), probe(url)]
        valid = [a["first_audio_ms"] for a in attempts if a["ok"] and a["first_audio_ms"] is not None]
        streams.append({"index": index, "url": url, "attempts": attempts,
            "passed": sum(a["ok"] for a in attempts),
            "median_ms": round(statistics.median(valid)) if valid else None})
    stable = [s for s in streams if s["passed"] == 2]
    partial = [s for s in streams if s["passed"] == 1]
    best = min(stable or partial, key=lambda s:s["median_ms"]) if (stable or partial) else None
    if stable:
        state = "SLOW" if best["median_ms"] > 3000 else "PASS"
    elif partial:
        state = "INTERMITTENT"
    elif any(a["likely"] for s in streams for a in s["attempts"]):
        state = "INCONCLUSIVE"
    else:
        state = "FAIL"
    return {"id": station["id"], "name": station["name"],
        "style": station.get("style", ""), "state": state,
        "first_audio_ms": best["median_ms"] if best else None,
        "best_url_index": best["index"] if best else None,
        "primary_problem": bool(streams and streams[0]["passed"] < 2),
        "streams": streams}


def candidates(station, known_urls, limit=3):
    name = station["name"]
    style = station.get("style", "").lower()
    genre = "mariachi" if "mariachi" in (name.lower()+" "+style) else "ranchera"
    found = []
    for field, value in (("name", name), ("tag", genre)):
        qs = urllib.parse.urlencode({field:value, "hidebroken":"true", "limit":35})
        try:
            records = download_json(RADIO_BROWSER+"?"+qs, timeout=10)
        except Exception:
            continue
        for entry in records:
            url = (entry.get("url_resolved") or entry.get("url") or "").strip()
            label = (entry.get("name") or "").strip()
            if not url.startswith(("http://", "https://")) or url in known_urls:
                continue
            if any(z["url"] == url for z in found):
                continue
            similarity = difflib.SequenceMatcher(None, name.casefold(), label.casefold()).ratio()
            tags = (entry.get("tags") or "").lower()
            if similarity < .65 and genre not in (label+" "+tags).lower():
                continue
            found.append({"name":label, "url":url, "similarity":round(similarity,2),
                "relation":"POSIBLE_MISMA_EMISORA" if similarity >= .82 else "OTRA_DEL_GENERO"})
        if len(found) >= 10:
            break
    found.sort(key=lambda x:(x["relation"] != "POSIBLE_MISMA_EMISORA", -x["similarity"]))
    verified = []
    for entry in found[:7]:
        checks = [probe(entry["url"]), probe(entry["url"])]
        if all(a["ok"] and a["first_audio_ms"] is not None and a["first_audio_ms"] <= 3000
               for a in checks):
            entry["first_audio_ms"] = round(statistics.median(a["first_audio_ms"] for a in checks))
            verified.append(entry)
        if len(verified) >= limit:
            break
    return verified


def report_all(catalog, results, prior, output, discover=True):
    output.mkdir(parents=True, exist_ok=True)
    day = dt.datetime.now(dt.timezone.utc).date().isoformat()
    old_history = (prior or {}).get("history", {})
    history = {}
    for st in results:
        previous = [v for v in old_history.get(st["id"], []) if v.get("day") != day]
        history[st["id"]] = (previous + [{"day":day, "state":st["state"],
            "first_audio_ms":st["first_audio_ms"]}])[-14:]
        st["failed_days"] = 0
        for val in reversed(history[st["id"]]):
            if val["state"] == "FAIL":
                st["failed_days"] += 1
            else:
                break
        st["success_pct"] = round(100 * sum(
            v["state"] in ("PASS", "SLOW") for v in history[st["id"]]) /
            len(history[st["id"]]), 1)
    counts = Counter(s["state"] for s in results)
    affected = [s for s in results if s["state"] in ("FAIL", "INTERMITTENT", "SLOW")]
    known = {url for s in catalog["stations"] for url in s.get("streams", [])}
    if discover and counts["FAIL"] <= len(results) // 4:
        for s in sorted(affected, key=lambda x:(x["state"] != "FAIL", x["state"] != "INTERMITTENT"))[:8]:
            s["alternatives"] = candidates(s, known)
    obj = {"generated_utc":dt.datetime.now(dt.timezone.utc).isoformat(),
        "catalog_version":catalog.get("catalogVersion", catalog.get("version")),
        "stations_checked":len(results), "urls_checked":sum(len(s["streams"]) for s in results),
        "attempts_per_url":2, "counts":dict(counts), "history":history, "stations":results,
        "notes":["Prueba desde runner GitHub, no garantiza reproduccion Android o Chile",
                 "Las alternativas son sugerencias; no se publican ni sustituyen automaticamente"]}
    (output/"health.json").write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf8")
    with (output/"health.csv").open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["emisora","estado","primera_senal_ms","url","correctos_de_2","motivos"])
        for s in results:
            for t in s["streams"]:
                w.writerow([s["name"], s["state"], t["median_ms"], t["url"], t["passed"],
                    " | ".join(a["reason"] for a in t["attempts"])])
    md = ["# Radio Mexico de Siempre - auditoria diaria", "",
        "Generado UTC: "+obj["generated_utc"], "",
        f"Catalogo v{obj['catalog_version']}: **{len(results)} emisoras** / **{obj['urls_checked']} URLs**.",
        f"**PASS {counts['PASS']} | SLOW {counts['SLOW']} | INTERMITTENT {counts['INTERMITTENT']} | INCONCLUSIVE {counts['INCONCLUSIVE']} | FAIL {counts['FAIL']}**",
        "", "Tiempo: hasta primeros bytes desde el runner; no es el tiempo de inicio audible en Android.",
        "SLOW: mas de 3000 ms. Dos intentos por URL.", "",
        "## Todas las emisoras", "",
        "| Emisora | Estado | Primera senal | Exito historico (hasta 14 dias) |",
        "|---|---|---:|---:|"]
    for s in results:
        timing = str(s["first_audio_ms"])+" ms" if s["first_audio_ms"] is not None else "No confirmada"
        md.append(f"| {s['name'].replace('|','/')} | {s['state']} | {timing} | {s['success_pct']}% |")
    md.extend(["", "## Revision de problemas", "",
        "Las alternativas NO se publican automaticamente.", ""])
    for s in affected:
        md.append(f"### {s['name']} - {s['state']} (fallas consecutivas: {s['failed_days']})")
        for url in s["streams"]:
            reasons = ", ".join(a["reason"] for a in url["attempts"])
            md.append(f"- URL {url['index']}: {url['passed']}/2; {reasons}")
        for c in s.get("alternatives", []):
            md.append(f"- Candidata {c['relation']}: {c['name']} - {c['first_audio_ms']} ms - {c['url']}")
        md.append("")
    (output/"HEALTH-REPORT.md").write_text("\n".join(md), encoding="utf8")
    return obj


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default=CATALOG)
    parser.add_argument("--previous", default="previous/health.json")
    parser.add_argument("--output", default="reports")
    parser.add_argument("--no-discovery", action="store_true")
    args = parser.parse_args()
    catalog = download_json(args.catalog)
    active = [s for s in catalog.get("stations", []) if s.get("enabled") is not False]
    if len(active) < 20 or len({s["id"] for s in active}) != len(active):
        raise ValueError("Catalogo invalido, incompleto o IDs duplicados")
    try:
        prior = json.loads(pathlib.Path(args.previous).read_text(encoding="utf8"))
    except (OSError, ValueError):
        prior = None
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        results = list(pool.map(check_station, active))
    output = report_all(catalog, results, prior, pathlib.Path(args.output), not args.no_discovery)
    print("RADIO_DAILY_QA", json.dumps({"counts":output["counts"],
          "stations":output["stations_checked"], "urls":output["urls_checked"]}), flush=True)
    return 2 if any(st["failed_days"] >= 2 for st in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
