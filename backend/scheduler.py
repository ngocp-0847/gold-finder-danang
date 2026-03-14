"""
Background scheduler for periodic gold price crawling.
Uses APScheduler to run price pipeline every 15 minutes.
"""

import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED

from crawlers.price_pipeline import run_pipeline
from database import SessionLocal

logger = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None


def _job_listener(event):
    if event.exception:
        logger.error(f"[scheduler] Job {event.job_id} raised an exception: {event.exception}")
    else:
        logger.debug(f"[scheduler] Job {event.job_id} executed successfully")


def run_price_job():
    """Single price crawl job — called by scheduler every 15 minutes."""
    db = SessionLocal()
    try:
        summary = run_pipeline(db)
        logger.info(f"[scheduler] Price update: {summary}")
    except Exception as e:
        logger.error(f"[scheduler] run_price_job error: {e}")
    finally:
        db.close()


def start_scheduler() -> BackgroundScheduler:
    """Start the background scheduler. Safe to call multiple times (idempotent)."""
    global _scheduler
    if _scheduler is not None and _scheduler.running:
        logger.info("[scheduler] Already running, skipping start")
        return _scheduler

    _scheduler = BackgroundScheduler(timezone="Asia/Ho_Chi_Minh")
    _scheduler.add_listener(_job_listener, EVENT_JOB_ERROR | EVENT_JOB_EXECUTED)
    _scheduler.add_job(
        run_price_job,
        trigger="interval",
        minutes=15,
        id="price_crawl",
        replace_existing=True,
        max_instances=1,
    )
    _scheduler.start()
    logger.info("[scheduler] Started — price crawl every 15 minutes")
    return _scheduler


def stop_scheduler():
    """Gracefully shut down the scheduler."""
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("[scheduler] Stopped")
    _scheduler = None
