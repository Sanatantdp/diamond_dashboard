import paramiko
import os
import logging
from apscheduler.schedulers.blocking import BlockingScheduler

# ── Logging ───────────────────────────────────────────────────────────────────
log_dir = os.path.join(os.getcwd(), "logs")
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(os.path.join(log_dir, "sftp_upload.log"), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("sftp_upload")

# ── Config ────────────────────────────────────────────────────────────────────
SFTP_HOST      = "122.176.221.68"
SFTP_PORT      = 22                  # SSH/SFTP default port
SFTP_USER      = "qnapadmin"
SFTP_PASSWORD  = "Passw0rd@123$"
SFTP_REMOTE_DIR = "/share/CACHEDEV1_DATA/Web/usa/compare"
LOCAL_FOLDER   = r"D:\usa_scrapping\compare"


# ── SFTP Upload ───────────────────────────────────────────────────────────────
def upload_folder():
    import datetime
    log.info("=" * 55)
    log.info(f"Upload job started — {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"Local folder  : {LOCAL_FOLDER}")
    log.info(f"Remote folder : {SFTP_REMOTE_DIR}")

    # Scan local files
    try:
        files = [
            f for f in os.listdir(LOCAL_FOLDER)
            if os.path.isfile(os.path.join(LOCAL_FOLDER, f))
        ]
    except Exception as e:
        log.error(f"Cannot read local folder: {e}")
        log.info("=" * 55)
        return

    if not files:
        log.warning("No files found — skipping.")
        log.info("=" * 55)
        return

    log.info(f"Files to upload: {len(files)}")

    # Connect via SFTP
    ssh = None
    sftp = None
    try:
        log.info(f"Connecting to {SFTP_HOST}:{SFTP_PORT} via SFTP...")
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # auto accept host key
        ssh.connect(
            hostname=SFTP_HOST,
            port=SFTP_PORT,
            username=SFTP_USER,
            password=SFTP_PASSWORD,
            timeout=15,
        )
        sftp = ssh.open_sftp()
        log.info(f"✓ Connected — logged in as {SFTP_USER}")

        # Ensure remote directory exists
        try:
            sftp.chdir(SFTP_REMOTE_DIR)
            log.info(f"✓ Remote dir exists: {SFTP_REMOTE_DIR}")
        except IOError:
            log.warning(f"Remote dir not found — creating: {SFTP_REMOTE_DIR}")
            _mkdir_recursive(sftp, SFTP_REMOTE_DIR)
            sftp.chdir(SFTP_REMOTE_DIR)

        # Upload each file
        success = 0
        failed  = 0

        for filename in files:
            local_path  = os.path.join(LOCAL_FOLDER, filename)
            remote_path = SFTP_REMOTE_DIR + "/" + filename
            size_kb     = os.path.getsize(local_path) / 1024
            try:
                sftp.put(local_path, remote_path)
                log.info(f"  ✓ Uploaded : {filename}  ({size_kb:.1f} KB)")
                success += 1
            except Exception as e:
                log.error(f"  ✗ Failed   : {filename}  — {e}")
                failed += 1

        log.info("-" * 55)
        log.info(f"Done — ✓ {success} uploaded  |  ✗ {failed} failed")

    except paramiko.AuthenticationException:
        log.error("✗ Authentication failed — check username/password")
    except paramiko.SSHException as e:
        log.error(f"✗ SSH error: {e}")
    except Exception as e:
        log.error(f"✗ Connection error: {e}")
    finally:
        if sftp:
            sftp.close()
        if ssh:
            ssh.close()
            log.info("Connection closed.")

    log.info("=" * 55)


def _mkdir_recursive(sftp, remote_path):
    """Create remote directory and all parents if they don't exist."""
    parts = remote_path.strip("/").split("/")
    current = ""
    for part in parts:
        current += "/" + part
        try:
            sftp.mkdir(current)
            log.info(f"  Created dir: {current}")
        except IOError:
            pass   # already exists


# ── Quick login test ──────────────────────────────────────────────────────────
def test_login():
    """Run this first to confirm SFTP is working before scheduling."""
    log.info("Testing SFTP login...")
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(SFTP_HOST, port=SFTP_PORT, username=SFTP_USER,
                    password=SFTP_PASSWORD, timeout=10)
        sftp = ssh.open_sftp()
        log.info(f"✓ LOGIN SUCCESSFUL as {SFTP_USER}")
        log.info(f"  Remote dir listing: {sftp.listdir('/share')}")
        sftp.close()
        ssh.close()
        return True
    except Exception as e:
        log.error(f"✗ Login failed: {e}")
        return False


# ── Scheduler ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("SFTP Upload Scheduler")
    log.info(f"Host   : {SFTP_HOST}:{SFTP_PORT}")
    log.info(f"User   : {SFTP_USER}")
    log.info(f"Local  : {LOCAL_FOLDER}")
    log.info(f"Remote : {SFTP_REMOTE_DIR}")

    # Step 1 — test login first
    if not test_login():
        log.error("Login test failed — fix connection before scheduling. Exiting.")
        exit(1)

    # Step 2 — run upload immediately
    upload_folder()

    # # Step 3 — schedule every 10 minutes
    # scheduler = BlockingScheduler()
    # scheduler.add_job(upload_folder, "interval", minutes=10)
    # # scheduler.add_job(upload_folder, "cron", hour=2)  # daily at 2am

    # try:
    #     scheduler.start()
    # except (KeyboardInterrupt, SystemExit):
    #     log.info("Scheduler stopped.")
    
    {"white": ["https://overnightmountings.s3.amazonaws.com/51138-W.jpg"], "yellow": ["https://overnightmountings.s3.amazonaws.com/51138-W.alt.jpg"],  "platinum": ["https://overnightmountings.s3.amazonaws.com/51138-W.jpg"]}	