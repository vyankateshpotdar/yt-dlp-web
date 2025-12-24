from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import yt_dlp
import tempfile
import os
import asyncio
import httpx
from typing import Optional
import shutil

# Required env vars
TURNSTILE_SECRET = os.getenv("TURNSTILE_SECRET")  # Set this on your VPS/container

if not TURNSTILE_SECRET:
    # In production you MUST set TURNSTILE_SECRET. For local dev you can set it as well.
    # If not set here, verification will fail.
    pass

app = FastAPI(title="YT Downloader API (with Turnstile)")

# Configure CORS - in production replace "*" with your Netlify origin (https://your-site.netlify.app)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Limit concurrent conversions/downloads
CONVERSION_SEMAPHORE = asyncio.Semaphore(1)


async def verify_turnstile(token: str, remoteip: Optional[str] = None) -> bool:
    """
    Verify a Turnstile token with Cloudflare.
    Returns True when verification succeeds.
    """
    if not TURNSTILE_SECRET:
        return False

    url = "https://challenges.cloudflare.com/turnstile/v0/siteverify"
    data = {
        "secret": TURNSTILE_SECRET,
        "response": token,
    }
    if remoteip:
        data["remoteip"] = remoteip

    async with httpx.AsyncClient(timeout=8.0) as client:
        r = await client.post(url, data=data)
    if r.status_code != 200:
        return False
    res = r.json()
    # res contains fields: success (bool), challenge_ts, hostname, error-codes etc.
    return bool(res.get("success"))


async def extract_turnstile_token(request: Request) -> Optional[str]:
    """
    Look for the turnstile token in header X-Turnstile-Token or query param cf-turnstile-response.
    Also checks JSON body for 'cf-turnstile-response'.
    """
    token = request.headers.get("X-Turnstile-Token")
    if token:
        return token
    token = request.query_params.get("cf-turnstile-response")
    if token:
        return token
    # try JSON body (for POST)
    try:
        body = await request.json()
        token = body.get("cf-turnstile-response")
        if token:
            return token
    except Exception:
        # no JSON body or parsing failed
        pass
    return None


async def async_stream_url(url: str, client_headers: dict = None, chunk_size: int = 1024 * 32):
    """
    Stream proxied content from `url`.
    """
    headers = client_headers or {}
    async with httpx.AsyncClient(timeout=600.0, follow_redirects=True) as client:
        r = await client.stream("GET", url, headers=headers)
        if r.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Upstream returned {r.status_code}")
        async for chunk in r.aiter_bytes(chunk_size):
            yield chunk


@app.get("/api/info")
async def info(request: Request, url: str = Query(..., description="YouTube (or supported) video URL")):
    """
    Return metadata and available formats for the URL.
    Requires a valid Cloudflare Turnstile token.
    """
    token = await extract_turnstile_token(request)
    client_ip = request.client.host if request.client else None
    if not token:
        raise HTTPException(status_code=403, detail="Missing Turnstile token")
    verified = await verify_turnstile(token, remoteip=client_ip)
    if not verified:
        raise HTTPException(status_code=403, detail="Turnstile verification failed")

    ydl_opts = {
        "skip_download": True,
        "quiet": True,
        "no_warnings": True,
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"yt-dlp error: {str(e)}")

    formats = []
    for f in info.get("formats", []):
        formats.append({
            "format_id": f.get("format_id"),
            "ext": f.get("ext"),
            "acodec": f.get("acodec"),
            "vcodec": f.get("vcodec"),
            "format_note": f.get("format_note"),
            "height": f.get("height"),
            "width": f.get("width"),
            "filesize": f.get("filesize") or f.get("filesize_approx"),
            # exposing direct urls is optional; consider removing for stricter security
            "url": f.get("url"),
        })

    resp = {
        "id": info.get("id"),
        "title": info.get("title"),
        "uploader": info.get("uploader"),
        "duration": info.get("duration"),
        "thumbnail": info.get("thumbnail"),
        "formats": formats,
    }
    return JSONResponse(resp)


@app.get("/api/download")
async def download(request: Request, url: str = Query(...), format_id: Optional[str] = Query(None), type: Optional[str] = Query("mp4")):
    """
    Stream a download. Requires a valid Turnstile token.
    """
    token = await extract_turnstile_token(request)
    client_ip = request.client.host if request.client else None
    if not token:
        raise HTTPException(status_code=403, detail="Missing Turnstile token")
    verified = await verify_turnstile(token, remoteip=client_ip)
    if not verified:
        raise HTTPException(status_code=403, detail="Turnstile verification failed")

    if type not in ("mp4", "mp3"):
        raise HTTPException(status_code=400, detail="type must be mp4 or mp3")

    # Fetch metadata to map format_id -> direct url
    ydl_opts = {"skip_download": True, "quiet": True, "no_warnings": True}
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"yt-dlp error: {str(e)}")

    chosen = None
    if format_id:
        for f in info.get("formats", []):
            if str(f.get("format_id")) == str(format_id):
                chosen = f
                break
    else:
        # pick best available
        formats_list = info.get("formats", [])
        if formats_list:
            chosen = formats_list[-1]
    if not chosen:
        raise HTTPException(status_code=404, detail="format not found")

    if type == "mp4" and chosen.get("url"):
        filename = f"{info.get('title')}.{chosen.get('ext')}"
        headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
        return StreamingResponse(async_stream_url(chosen["url"]), media_type="application/octet-stream", headers=headers)

    if type == "mp3":
        if CONVERSION_SEMAPHORE.locked():
            raise HTTPException(status_code=429, detail="Server busy, try again later")
        await CONVERSION_SEMAPHORE.acquire()
        tmpdir = tempfile.mkdtemp(prefix="ytdl_")
        try:
            outtmpl = os.path.join(tmpdir, "%(title)s.%(ext)s")
            ydl_opts_conv = {
                "format": "bestaudio/best",
                "outtmpl": outtmpl,
                "quiet": True,
                "no_warnings": True,
                "postprocessors": [{
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": "mp3",
                    "preferredquality": "192",
                }],
            }
            with yt_dlp.YoutubeDL(ydl_opts_conv) as ydl:
                result = ydl.extract_info(url, download=True)
            produced_files = [os.path.join(tmpdir, f) for f in os.listdir(tmpdir)]
            mp3_files = [f for f in produced_files if f.lower().endswith(".mp3")]
            if not mp3_files:
                raise HTTPException(status_code=500, detail="Conversion failed (no mp3 created)")
            mp3_path = mp3_files[0]
            fname = os.path.basename(mp3_path)
            headers = {"Content-Disposition": f'attachment; filename="{fname}"'}

            def file_iterator(path, chunk_size=1024 * 32):
                with open(path, "rb") as fh:
                    while True:
                        chunk = fh.read(chunk_size)
                        if not chunk:
                            break
                        yield chunk

            return StreamingResponse(file_iterator(mp3_path), media_type="audio/mpeg", headers=headers)
        except yt_dlp.utils.DownloadError as e:
            raise HTTPException(status_code=400, detail=f"yt-dlp error: {str(e)}")
        finally:
            try:
                shutil.rmtree(tmpdir)
            except Exception:
                pass
            CONVERSION_SEMAPHORE.release()
