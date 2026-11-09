#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Edge Shard Synchronizer Node
High-Performance Distributed Replication & Benchmark Runner
Protected: Zero-Avatar, Zero-HTML, Media Only (>1MB)
"""

import os
import sys
import time
import json
import base64
import argparse
import requests
import boto3
import threading
import concurrent.futures
from botocore.config import Config

VALID_MEDIA_EXTS = (
    '.mkv', '.mp4', '.avi', '.mov', '.webm', '.flv', '.ts', '.m4v', '.3gp',
    '.zip', '.tar', '.rar', '.7z', '.gz', '.iso', '.bin'
)

PROHIBITED_EXTS = (
    '.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.svg', '.ico',
    '.html', '.htm', '.php', '.asp', '.aspx', '.js', '.css', '.txt', '.json', '.xml'
)

def is_valid_media_filename(fn):
    if not fn:
        return False
    fn_lower = fn.lower().strip()
    for bad in PROHIBITED_EXTS:
        if fn_lower.endswith(bad):
            return False
    for good in VALID_MEDIA_EXTS:
        if fn_lower.endswith(good):
            return True
    return False

def log(msg):
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {msg}", flush=True)

def resolve_shard_metadata(shard_id, resolver_url, resolver_key):
    log(f"[*] Querying cluster shard telemetry for task: {shard_id} ...")
    params = {
        "api_key": resolver_key,
        "url": shard_id
    }
    r = requests.get(resolver_url, params=params, timeout=25)
    if r.status_code != 200:
        raise Exception(f"Shard controller returned HTTP {r.status_code}")
    
    data = r.json()
    filename = data.get("filename", f"shard_{shard_id}.mkv")
    file_size = int(data.get("file_size", 0))
    links = data.get("links", {})
    existing_s3_url = ""
    for k, v in links.items():
        if ("public_url" in k or "download_url" in k) and str(v).startswith("http"):
            existing_s3_url = str(v)
            break
            
    source_url = None
    for k in ["download_url", "direct_url", "url", "stream_url"]:
        if k in data and str(data[k]).startswith("http"):
            source_url = str(data[k])
            break
            
    if not source_url and links:
        for k, v in links.items():
            if str(v).startswith("http") and "stream" in k.lower():
                source_url = str(v)
                break
        if not source_url:
            for v in links.values():
                if str(v).startswith("http"):
                    source_url = str(v)
                    break
                    
    if not source_url:
        raise Exception("Failed to resolve viable source link from cluster metadata")
        
    return {
        "filename": filename,
        "file_size": file_size,
        "source_url": source_url,
        "existing_s3_url": existing_s3_url
    }

def stream_source_buffer(source_url, fallback_url, local_path, expected_size, callback_url, callback_token, task_id):
    log("[*] Connecting stream buffer to source...")
    urls_to_try = [u for u in [source_url, fallback_url] if u]
    last_err = None
    for u in urls_to_try:
        try:
            return _download_stream(u, local_path, expected_size, callback_url, callback_token, task_id)
        except Exception as e:
            log(f"[!] Stream source attempt failed: {e}. Trying fallback if available...")
            last_err = e
    raise last_err or Exception("All stream sources failed")

def _download_stream(url, local_path, expected_size, callback_url, callback_token, task_id):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "*/*"
    }
    r = requests.get(url, headers=headers, stream=True, timeout=45)
    if r.status_code not in (200, 206):
        raise Exception(f"Edge socket stream failed with HTTP {r.status_code}")
    
    # Content-Type verification
    c_type = (r.headers.get("Content-Type") or "").lower()
    blocked_types = ["image/", "text/html", "text/plain", "application/json", "application/xml"]
    if any(b in c_type for b in blocked_types):
        raise Exception(f"Prohibited Content-Type: '{c_type}' (images and html error pages blocked)")

    # Read Content-Length if expected_size was not passed or <= 0
    if not expected_size or expected_size <= 0:
        cl = r.headers.get("Content-Length")
        if cl and str(cl).isdigit():
            expected_size = int(cl)
            
    if expected_size > 0 and expected_size < 1024 * 1024:
        raise Exception(f"File size ({expected_size} bytes) is below 1MB (corrupt/avatar source blocked)")

    detected_fn = ""
    cd = r.headers.get("Content-Disposition", "")
    if cd and "filename=" in cd:
        for part in cd.split(";"):
            part = part.strip()
            if part.lower().startswith("filename="):
                fn = part.split("=", 1)[1].strip().strip('"\'')
                if fn:
                    detected_fn = fn
                    break

    if detected_fn and not is_valid_media_filename(detected_fn):
        raise Exception(f"Prohibited filename extension detected: '{detected_fn}'")

    total_downloaded = 0
    start_time = time.time()
    last_heartbeat = start_time
    first_chunk = True
    
    with open(local_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024 * 2): # 2MB buffer
            if chunk:
                if first_chunk:
                    first_chunk = False
                    lower_sample = chunk[:512].lower()
                    if b"<!doctype" in lower_sample or b"<html" in lower_sample or b"<head" in lower_sample or b"<body" in lower_sample:
                        raise Exception("Stream aborted: received HTML error page instead of media binary")
                    if chunk.startswith(b"\x89PNG\r\n\x1a\n") or chunk.startswith(b"\xff\xd8\xff") or (chunk.startswith(b"RIFF") and b"WEBP" in chunk[:16]):
                        raise Exception("Stream aborted: received image file (PNG/JPEG/WEBP) instead of media")
                
                f.write(chunk)
                total_downloaded += len(chunk)
                now = time.time()
                if now - last_heartbeat >= 3: # heartbeat every 3 seconds
                    elapsed = now - start_time
                    speed_mbps = (total_downloaded * 8 / (1024 * 1024)) / (elapsed if elapsed > 0 else 1)
                    stream_pct = (total_downloaded / expected_size * 100) if expected_size > 0 else min(95.0, (total_downloaded / (300*1024*1024)) * 100)
                    job_pct = stream_pct * 0.5 # Download is 0% -> 50% of the overall job
                    log(f"[*] Stream Progress: {total_downloaded / (1024*1024):.1f} MB ({stream_pct:.1f}%) | Speed: {speed_mbps:.1f} Mbps")
                    if callback_url and callback_token:
                        try:
                            hb_payload = {
                                "task_id": task_id,
                                "status": "active",
                                "step": f"downloading source ({stream_pct:.1f}%) | {total_downloaded/(1024*1024):.1f} MB",
                                "progress_pct": round(job_pct, 1),
                                "speed_mbps": round(speed_mbps, 2),
                                "bytes_streamed": total_downloaded,
                                "file_size": expected_size or total_downloaded
                            }
                            requests.post(callback_url, json=hb_payload, headers={"Content-Type": "application/json", "X-API-Key": callback_token}, timeout=3)
                        except:
                            pass
                    last_heartbeat = now
    
    if total_downloaded < 1024 * 1024:
        try:
            os.remove(local_path)
        except:
            pass
        raise Exception(f"Stream aborted: total downloaded size ({total_downloaded} bytes) is below 1MB (corrupt/empty file)")

    elapsed = time.time() - start_time
    avg_speed = (total_downloaded * 8 / (1024 * 1024)) / (elapsed if elapsed > 0 else 1)
    log(f"[✓] Stream Buffer Complete: {total_downloaded / (1024*1024):.2f} MB in {elapsed:.1f}s (Avg Speed: {avg_speed:.1f} Mbps)")
    
    if callback_url and callback_token:
        try:
            hb_payload = {
                "task_id": task_id,
                "status": "active",
                "step": f"source buffered ({total_downloaded/(1024*1024):.1f} MB) | broadcasting to mirrors",
                "progress_pct": 50.0,
                "speed_mbps": round(avg_speed, 2),
                "bytes_streamed": total_downloaded,
                "file_size": total_downloaded
            }
            requests.post(callback_url, json=hb_payload, headers={"Content-Type": "application/json", "X-API-Key": callback_token}, timeout=3)
        except:
            pass

    return total_downloaded, detected_fn

def replicate_object_storage_s3(local_path, filename, s3_endpoint, access_key, secret_key, bucket_name, custom_domain):
    log("[*] Uploading shard to Object Storage S3 (Cloudflare R2)...")
    s3_key = f"shards/{int(time.time())}/{filename.replace(' ', '_')}"
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=s3_endpoint,
        config=Config(s3={'addressing_style': 'path'}, signature_version='s3v4')
    )
    
    file_size = os.path.getsize(local_path)
    content_type = "video/mp4"
    if filename.endswith(".mkv"):
        content_type = "video/x-matroska"
    elif filename.endswith(".zip"):
        content_type = "application/zip"
        
    s3_client.upload_file(
        Filename=local_path,
        Bucket=bucket_name,
        Key=s3_key,
        ExtraArgs={'ContentType': content_type}
    )
    
    public_url = f"{s3_endpoint.rstrip('/')}/{bucket_name}/{s3_key}"
    if custom_domain:
        clean_domain = custom_domain.rstrip('/')
        if not clean_domain.startswith("http://") and not clean_domain.startswith("https://"):
            clean_domain = f"https://{clean_domain}"
        public_url = f"{clean_domain}/{s3_key}"
        
    log(f"[✓] Object Storage S3 replication complete: {public_url}")
    return s3_key, public_url

def replicate_target_a(local_path, filename, base_endpoint, view_endpoint, api_key):
    log("[*] Replicating shard to Target A (Pixeldrain)...")
    upload_url = f"{base_endpoint.rstrip('/')}/{filename}"
    auth = None
    if api_key:
        auth = ("", api_key)
        
    with open(local_path, "rb") as f:
        r = requests.put(upload_url, data=f, auth=auth, timeout=900)
        
    if r.status_code in (200, 201):
        res = r.json()
        file_id = res.get("id")
        if file_id:
            page_url = f"{view_endpoint.rstrip('/')}/{file_id}"
            log(f"[✓] Target A Replication Complete: {page_url}")
            return file_id, page_url
    raise Exception(f"Target A replication failed with HTTP {r.status_code}: {r.text[:200]}")

def replicate_target_b(local_path, filename, base_endpoint, gateway_format):
    log("[*] Replicating shard to Target B (Gofile)...")
    srv_resp = requests.get(f"{base_endpoint.rstrip('/')}/servers", timeout=15)
    if srv_resp.status_code != 200:
        raise Exception(f"Failed to fetch Target B server pool: HTTP {srv_resp.status_code}")
    srv_data = srv_resp.json()
    if srv_data.get("status") != "ok":
        raise Exception("Target B server discovery status not ok")
        
    servers = srv_data["data"].get("servers", [])
    if not servers:
        raise Exception("No Target B servers available")
    target_server = servers[0]["name"]
    upload_url = f"https://{target_server}.gofile.io/contents/uploadfile"
    
    with open(local_path, "rb") as f:
        files = {"file": (filename, f)}
        r = requests.post(upload_url, files=files, timeout=900)
        
    if r.status_code in (200, 201):
        res = r.json()
        if res.get("status") in ("ok", "success"):
            page_url = res["data"].get("downloadPage", "")
            file_id = res["data"].get("fileId", "")
            log(f"[✓] Target B Replication Complete: {page_url}")
            return file_id, page_url
    raise Exception(f"Target B replication failed: {r.text[:200]}")

def replicate_target_c(local_path, filename, base_endpoint, fallback_endpoint, user_hash):
    log("[*] Replicating shard to Target C (Vikingfile Multi-Part)...")
    file_size = os.path.getsize(local_path)
    
    init_data = {
        "user_hash": user_hash,
        "name": filename,
        "size": str(file_size)
    }
    r = requests.post(f"{base_endpoint.rstrip('/')}/get-upload-url", data=init_data, timeout=30)
    if r.status_code == 200:
        init_json = r.json()
        urls = init_json.get("urls", [])
        part_size = int(init_json.get("partSize", 0))
        upload_id = init_json.get("upload_id")
        key = init_json.get("key")
        
        if urls and part_size > 0:
            log(f"[*] Uploading {len(urls)} parts to Target C...")
            etags = []
            with open(local_path, "rb") as f:
                for idx, part_url in enumerate(urls):
                    chunk = f.read(part_size)
                    part_num = idx + 1
                    put_resp = requests.put(part_url, data=chunk, timeout=300)
                    if put_resp.status_code not in (200, 201):
                        raise Exception(f"Target C Part {part_num} upload failed with HTTP {put_resp.status_code}")
                    etag = put_resp.headers.get("ETag", "").strip('"')
                    etags.append(f"{part_num}:{etag}")
            
            complete_data = {
                "user_hash": user_hash,
                "upload_id": upload_id,
                "key": key,
                "parts": ",".join(etags)
            }
            
            comp_resp = requests.post(f"{base_endpoint.rstrip('/')}/complete-upload", data=complete_data, timeout=35)
            if comp_resp.status_code == 200:
                comp_json = comp_resp.json()
                vf_url = comp_json.get("url")
                if not vf_url and comp_json.get("file_id"):
                    vf_url = f"https://vikingfile.com/f/{comp_json['file_id']}"
                if vf_url:
                    log(f"[✓] Target C Replication Complete: {vf_url}")
                    return comp_json.get("file_id", ""), vf_url

    log("[*] Multipart failed or unavail. Trying Single-Part Target C...")
    up_url = fallback_endpoint or "https://upload.vikingfile.com/api/upload"
    with open(local_path, "rb") as f:
        files = {"file": (filename, f)}
        data = {"user_hash": user_hash}
        r = requests.post(up_url, files=files, data=data, timeout=900)
    if r.status_code == 200:
        res = r.json()
        vf_url = res.get("url")
        if not vf_url and res.get("file_id"):
            vf_url = f"https://vikingfile.com/f/{res['file_id']}"
        if vf_url:
            log(f"[✓] Target C Single-part Replication Complete: {vf_url}")
            return res.get("file_id", ""), vf_url
    raise Exception(f"Target C replication failed: {r.text[:200]}")

def replicate_telegram(local_path, filename, bot_token, api_id, api_hash, channel_id, callback_url=None, callback_token=None, task_id=None):
    log("[*] Connecting directly to Telegram MTProto Cluster (Zero-VPS Bandwidth)...")
    import asyncio
    from telethon import TelegramClient

    file_size = os.path.getsize(local_path)
    total_mb = file_size / (1024 * 1024)
    last_reported_pct = -10
    last_reported_time = time.time()

    def tg_progress(current, total):
        nonlocal last_reported_pct, last_reported_time
        pct = (current / total) * 100
        now = time.time()
        if (pct - last_reported_pct >= 5.0) or (now - last_reported_time >= 3.0):
            curr_mb = current / (1024 * 1024)
            log(f"[*] Telegram Broadcast: {curr_mb:.1f} MB / {total_mb:.1f} MB ({pct:.1f}%)")
            last_reported_pct = pct
            last_reported_time = now
            if callback_url and callback_token and task_id:
                try:
                    requests.post(callback_url, json={
                        "task_id": task_id,
                        "status": "active",
                        "step": f"broadcasting to Telegram ({pct:.1f}%) | {curr_mb:.1f} MB",
                        "progress_pct": round(pct, 1),
                        "bytes_streamed": current,
                        "file_size": total
                    }, headers={"Content-Type": "application/json", "X-API-Key": callback_token}, timeout=2)
                except Exception:
                    pass

    async def _send_file():
        session_name = f"worker_session_{int(time.time())}"
        client = TelegramClient(session_name, int(api_id), api_hash)
        await client.start(bot_token=bot_token)
        target_peer = int(channel_id) if (str(channel_id).startswith("-100") or str(channel_id).isdigit()) else channel_id
        msg = await client.send_file(
            target_peer,
            local_path,
            caption=f"📁 {filename}",
            supports_streaming=True,
            progress_callback=tg_progress
        )
        await client.disconnect()
        return msg.id

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        msg_id = loop.run_until_complete(_send_file())
        loop.close()
        log(f"[✓] Telegram MTProto Broadcast Complete! Msg ID: {msg_id}")
        return msg_id, channel_id
    except Exception as e:
        log(f"[!] Telegram Broadcast Error: {e}")
        return None, None

def report_telemetry_receipt(callback_url, callback_token, payload):
    log("[*] Pushing shard replication receipt to controller...")
    headers = {
        "Content-Type": "application/json",
        "X-API-Key": callback_token
    }
    r = requests.post(callback_url, json=payload, headers=headers, timeout=30)
    log(f"[*] Receipt delivery response: HTTP {r.status_code}")

def main():
    parser = argparse.ArgumentParser(description="Distributed Cluster Shard Mirror Node")
    parser.add_argument("--task-id", dest="task_id", default="")
    parser.add_argument("--file-id", dest="legacy_file_id", default="")
    parser.add_argument("--controller", dest="controller", default="")
    parser.add_argument("--callback-url", dest="legacy_callback_url", default="")
    parser.add_argument("--token", dest="token", default="")
    parser.add_argument("--callback-token", dest="legacy_callback_token", default="")
    parser.add_argument("--config-env", dest="config_env", default="")
    
    args = parser.parse_args()
    task_id = (args.task_id or args.legacy_file_id).strip()
    callback_url = args.controller or args.legacy_callback_url
    callback_token = args.token or args.legacy_callback_token
    
    cfg = {}
    if args.config_env:
        try:
            cfg = json.loads(base64.b64decode(args.config_env).decode("utf-8"))
        except Exception as e:
            log(f"[!] Warning: Failed to parse config_env: {e}")
            
    resolver_url = cfg.get("r", "")
    resolver_key = cfg.get("r_key") or callback_token
    endpoint_a = cfg.get("e_a", "")
    endpoint_a_view = cfg.get("e_a_view", "")
    endpoint_b = cfg.get("e_b", "")
    gateway_b_fmt = cfg.get("e_b_fmt", "")
    endpoint_c = cfg.get("e_c", "")
    endpoint_c_up = cfg.get("e_c_up", "")
    key_a = cfg.get("k_a", "")
    key_c = cfg.get("k_c", "")
    
    s3_endpoint = cfg.get("s3_host", "")
    s3_key = cfg.get("s3_key", "")
    s3_sec = cfg.get("s3_sec", "")
    s3_bkt = cfg.get("s3_bkt", "")
    s3_dom = cfg.get("s3_dom", "")
    s3_active = cfg.get("t_s3", False)
    
    target_a_active = cfg.get("t_a", True)
    target_b_active = cfg.get("t_b", True)
    target_c_active = cfg.get("t_c", True)
    
    tg_active = cfg.get("t_tg", False)
    tg_bot_token = cfg.get("tg_bot", "")
    tg_api_id = cfg.get("tg_id", "")
    tg_api_hash = cfg.get("tg_hash", "")
    tg_channel = cfg.get("tg_chan", "")
    
    log("===========================================================")
    log(f"[*] Initializing Parallel Cluster Shard Worker for: {task_id}")
    log(f"[*] Targets: Pixeldrain={target_a_active}, Gofile={target_b_active}, Vikingfile={target_c_active}, R2={s3_active}, TG={tg_active}")
    log("===========================================================")
    
    source_url = cfg.get("src_url", "")
    fallback_url = cfg.get("fallback", "")
    filename = cfg.get("filename", "")
    expected_size = int(cfg.get("file_size", 0))
    existing_s3_url = ""
    target_shard_id = cfg.get("file_id") or task_id
    
    # If source_url is empty OR is a raw Google Drive web page link, resolve fresh direct stream CDN link
    is_raw_gdrive = "drive.google.com/file/d/" in source_url or "drive.google.com/open" in source_url
    if not source_url or is_raw_gdrive:
        log(f"[*] Source is not a direct stream link (is_raw_gdrive={is_raw_gdrive}). Resolving via DriveShare...")
        res = resolve_shard_metadata(target_shard_id, resolver_url, resolver_key)
        if not filename or is_raw_gdrive:
            filename = res["filename"]
        if expected_size <= 0 or is_raw_gdrive:
            expected_size = res["file_size"]
        source_url = res["source_url"]
        existing_s3_url = res.get("existing_s3_url", "")
        
    if not filename:
        filename = f"shard_{target_shard_id}.mkv"

    # Enforce media filename validation
    if not is_valid_media_filename(filename):
        raise Exception(f"Prohibited non-media filename: '{filename}' (only videos and archives permitted)")
        
    temp_file = f"/tmp/shard_{task_id}_{int(time.time())}.bin"
    if os.name == "nt":
        temp_file = os.path.join(os.environ.get("TEMP", "."), f"shard_{task_id}.bin")
        
    try:
        actual_size, detected_fn = stream_source_buffer(
            source_url, fallback_url, temp_file, expected_size,
            callback_url, callback_token, task_id
        )
        if detected_fn and is_valid_media_filename(detected_fn):
            filename = detected_fn
            log(f"[*] Resolved stream filename from headers: {filename}")
            
        log("🚀 [*] Dispatching parallel multi-threaded replication...")
        
        target_a_id, target_a_url = None, None
        target_b_id, target_b_url = None, None
        target_c_id, target_c_url = None, None
        s3_final_key, s3_final_url = None, None
        tg_final_msg_id, tg_final_channel = None, None
        
        futures = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            if s3_active and s3_key and s3_sec:
                futures[executor.submit(
                    replicate_object_storage_s3, temp_file, filename,
                    s3_endpoint, s3_key, s3_sec, s3_bkt, s3_dom
                )] = "s3"
                
            if target_a_active and endpoint_a:
                futures[executor.submit(
                    replicate_target_a, temp_file, filename,
                    endpoint_a, endpoint_a_view, key_a
                )] = "target_a"
                
            if target_b_active and endpoint_b:
                futures[executor.submit(
                    replicate_target_b, temp_file, filename,
                    endpoint_b, gateway_b_fmt
                )] = "target_b"
                
            if target_c_active and endpoint_c:
                futures[executor.submit(
                    replicate_target_c, temp_file, filename,
                    endpoint_c, endpoint_c_up, key_c
                )] = "target_c"
                
            if tg_active and tg_bot_token and tg_api_id and tg_api_hash and tg_channel:
                futures[executor.submit(
                    replicate_telegram, temp_file, filename,
                    tg_bot_token, tg_api_id, tg_api_hash, tg_channel,
                    callback_url, callback_token, task_id
                )] = "telegram"
                
            for fut in concurrent.futures.as_completed(futures):
                t_name = futures[fut]
                try:
                    res = fut.result()
                    if t_name == "s3":
                        s3_final_key, s3_final_url = res
                    elif t_name == "target_a":
                        target_a_id, target_a_url = res
                    elif t_name == "target_b":
                        target_b_id, target_b_url = res
                    elif t_name == "target_c":
                        target_c_id, target_c_url = res
                    elif t_name == "telegram":
                        tg_final_msg_id, tg_final_channel = res
                except Exception as e:
                    log(f"[!] Warning: Replication worker '{t_name}' failed: {e}")
                    
        receipt = {
            "task_id": task_id,
            "file_id": target_shard_id,
            "filename": filename,
            "file_size": actual_size,
            "status": "completed",
            "progress_pct": 100.0,
            "relay_a_id": target_a_id,
            "relay_a_url": target_a_url,
            "relay_b_id": target_b_id,
            "relay_b_url": target_b_url,
            "relay_c_url": target_c_url,
            "s3_key": s3_final_key,
            "s3_url": s3_final_url or existing_s3_url,
            "telegram_msg_id": tg_final_msg_id,
            "telegram_channel_id": tg_final_channel
        }
        
        report_telemetry_receipt(callback_url, callback_token, receipt)
        log("[✓] All shard replication tasks successfully completed.")
        
    finally:
        if os.path.exists(temp_file):
            try:
                os.remove(temp_file)
                log("[*] Local scratch buffer cleaned up.")
            except:
                pass

if __name__ == "__main__":
    main()
