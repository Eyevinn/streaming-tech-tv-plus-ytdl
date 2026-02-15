"""
YouTube video downloader microservice for Streaming Tech TV+.

Accepts a YouTube URL, downloads the video using yt-dlp,
uploads it to MinIO (S3-compatible), and returns the MinIO URL.
"""

import os
import uuid
import tempfile
import subprocess
import logging
from flask import Flask, request, jsonify
import boto3
from botocore.config import Config as BotoConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configuration from environment
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "source")
API_SECRET = os.environ.get("API_SECRET", "")

# Optional: load config from App Config Service
CONFIG_SERVICE_URL = os.environ.get("APP_CONFIG_URL", "")


def load_config_from_service():
    """Load configuration from App Config Service if available."""
    global MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, API_SECRET
    if not CONFIG_SERVICE_URL:
        return

    import requests

    keys = {
        "MINIO_ENDPOINT": "MINIO_ENDPOINT",
        "MINIO_ACCESS_KEY": "MINIO_ACCESS_KEY",
        "MINIO_SECRET_KEY": "MINIO_SECRET_KEY",
        "YT_DLP_API_SECRET": "API_SECRET",
    }
    for config_key, var_name in keys.items():
        try:
            res = requests.get(f"{CONFIG_SERVICE_URL}/api/v1/config/{config_key}", timeout=5)
            if res.ok:
                data = res.json()
                if data.get("value"):
                    globals()[var_name] = data["value"]
                    logger.info(f"Loaded {config_key} from config service")
        except Exception as e:
            logger.warning(f"Failed to load {config_key}: {e}")


def get_s3_client():
    """Create an S3 client configured for MinIO."""
    endpoint = MINIO_ENDPOINT.rstrip("/")
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=BotoConfig(signature_version="s3v4"),
        region_name="us-east-1",
    )


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@app.route("/api/download", methods=["POST"])
def download_video():
    """
    Download a YouTube video and upload to MinIO.

    Request body:
    {
        "url": "https://www.youtube.com/watch?v=...",
        "videoId": "optional-video-id",  // used as filename prefix
    }

    Response:
    {
        "sourceUrl": "https://minio.../source/youtube/abc123.mp4",
        "duration": 1234,
        "fileSize": 56789000
    }
    """
    # Simple auth check
    if API_SECRET:
        auth = request.headers.get("Authorization", "")
        if auth != f"Bearer {API_SECRET}":
            return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json()
    if not data or not data.get("url"):
        return jsonify({"error": "url is required"}), 400

    url = data["url"]
    video_id = data.get("videoId", str(uuid.uuid4())[:8])

    logger.info(f"Starting download for {url} (videoId: {video_id})")

    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = os.path.join(tmpdir, f"{video_id}.%(ext)s")

        # Download with yt-dlp
        # Use best mp4 format, or best video+audio merged to mp4
        cmd = [
            "yt-dlp",
            "--no-playlist",
            "--js-runtimes", "node",
            "--force-ipv4",
            "--extractor-args", "youtube:player_client=web_music,web",
            "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "-f", "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
            "--merge-output-format", "mp4",
            "-o", output_path,
            "--no-progress",
            "--print-json",
            url,
        ]

        logger.info(f"Running: {' '.join(cmd)}")

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=600,  # 10 minute timeout
            )
        except subprocess.TimeoutExpired:
            return jsonify({"error": "Download timed out (10 min limit)"}), 504

        if result.returncode != 0:
            logger.error(f"yt-dlp stderr: {result.stderr}")
            return jsonify({
                "error": f"Download failed: {result.stderr[:500]}"
            }), 500

        # Parse yt-dlp JSON output for metadata
        import json
        try:
            metadata = json.loads(result.stdout.strip().split("\n")[-1])
            duration = metadata.get("duration", 0)
        except (json.JSONDecodeError, IndexError):
            duration = 0

        # Find the downloaded file
        downloaded_files = [
            f for f in os.listdir(tmpdir)
            if f.endswith((".mp4", ".mkv", ".webm"))
        ]

        if not downloaded_files:
            return jsonify({"error": "No video file found after download"}), 500

        local_file = os.path.join(tmpdir, downloaded_files[0])
        file_size = os.path.getsize(local_file)
        logger.info(f"Downloaded: {downloaded_files[0]} ({file_size / 1024 / 1024:.1f} MB)")

        # Upload to MinIO
        object_key = f"youtube/{video_id}.mp4"
        try:
            s3 = get_s3_client()
            s3.upload_file(
                local_file,
                MINIO_BUCKET,
                object_key,
                ExtraArgs={"ContentType": "video/mp4"},
            )
        except Exception as e:
            logger.error(f"MinIO upload failed: {e}")
            return jsonify({"error": f"Upload to storage failed: {str(e)}"}), 500

        source_url = f"{MINIO_ENDPOINT.rstrip('/')}/{MINIO_BUCKET}/{object_key}"
        logger.info(f"Uploaded to {source_url}")

        return jsonify({
            "sourceUrl": source_url,
            "duration": int(duration),
            "fileSize": file_size,
        })


# Load config from service on module import (for gunicorn)
load_config_from_service()
logger.info(f"MinIO endpoint: {MINIO_ENDPOINT or '(not set)'}")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    logger.info(f"Starting yt-dlp service on port {port}")
    app.run(host="0.0.0.0", port=port)
