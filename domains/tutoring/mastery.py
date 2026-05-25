"""Mastery tracking for tutoring: knowledge point mastery, spaced repetition,
weak point analysis, and report generation.

Data is stored as JSON files in MASTERY_DIR (default: /data/mastery).
Each learner has a file named {base64(learner_id)}.json.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)

MASTERY_DIR = os.getenv("MASTERY_DIR", "/data/mastery")
MASTERY_FILE = os.getenv("MASTERY_FILE", "")  # optional single-file override


def _learner_path(learner_id: str) -> str:
    b64 = base64.urlsafe_b64encode(learner_id.encode()).decode().rstrip("=")
    return os.path.join(MASTERY_DIR, f"{b64}.json")


def _load(learner_id: str) -> dict[str, Any]:
    """Load mastery data for a learner. Returns default structure if none exists."""
    path = _learner_path(learner_id)
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning("Failed to load mastery for %s: %s", learner_id, e)
    return {
        "learner_id": learner_id,
        "version": 1,
        "mastery": {},
        "wrong_answers": [],
        "total_questions": 0,
        "correct_count": 0,
        "daily_stats": {},
        "answer_history": [],
        "review_schedule": {},
        "review_history": [],
        "updated_at": time.time(),
    }


def _save(data: dict[str, Any]) -> None:
    os.makedirs(MASTERY_DIR, exist_ok=True)
    path = _learner_path(data["learner_id"])
    tmp = path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except Exception as e:
        logger.error("Failed to save mastery for %s: %s", data["learner_id"], e)


def get_mastery(learner_id: str, kp_id: str) -> dict[str, Any]:
    """Get mastery data for a specific knowledge point."""
    data = _load(learner_id)
    kp = data["mastery"].get(kp_id, {"level": 0.0, "total": 0, "correct": 0})
    return kp


def get_mastery_summary(learner_id: str) -> list[dict[str, Any]]:
    """Return summary of all knowledge points with mastery levels."""
    data = _load(learner_id)
    result = []
    for kp_id, kp in data["mastery"].items():
        pct = kp["correct"] / kp["total"] if kp["total"] > 0 else 0
        result.append({
            "kp_id": kp_id,
            "level": kp["level"],
            "total": kp["total"],
            "correct": kp["correct"],
            "accuracy": round(pct, 2),
        })
    return result


def update_mastery(
    learner_id: str,
    kp_id: str,
    correct: bool,
    question: str = "",
    user_answer: str = "",
    correct_answer: str = "",
) -> dict[str, Any]:
    """Record a mastery update for a knowledge point."""
    data = _load(learner_id)
    os.makedirs(MASTERY_DIR, exist_ok=True)

    if kp_id not in data["mastery"]:
        data["mastery"][kp_id] = {"level": 0.0, "total": 0, "correct": 0}

    kp = data["mastery"][kp_id]
    kp["total"] += 1
    if correct:
        kp["correct"] += 1
    # Level = accuracy
    kp["level"] = kp["correct"] / kp["total"] if kp["total"] > 0 else 0.0

    data["total_questions"] += 1
    if correct:
        data["correct_count"] += 1

    # Track wrong answers
    if not correct:
        data["wrong_answers"].append({
            "kp_id": kp_id,
            "question": question,
            "user_answer": user_answer,
            "correct_answer": correct_answer,
            "ts": time.time(),
        })
        # Keep only last 50 wrong answers
        if len(data["wrong_answers"]) > 50:
            data["wrong_answers"] = data["wrong_answers"][-50:]

    # Answer history
    data["answer_history"].append({
        "kp_id": kp_id,
        "question": question,
        "user_answer": user_answer,
        "correct_answer": correct_answer,
        "is_correct": correct,
        "ts": time.time(),
    })
    if len(data["answer_history"]) > 200:
        data["answer_history"] = data["answer_history"][-200:]

    # Daily stats
    today = date.today().isoformat()
    if today not in data["daily_stats"]:
        data["daily_stats"][today] = {"total": 0, "correct": 0, "wrong": 0, "weak_points": []}
    ds = data["daily_stats"][today]
    ds["total"] += 1
    if correct:
        ds["correct"] += 1
    else:
        ds["wrong"] += 1
        if kp_id not in ds["weak_points"]:
            ds["weak_points"].append(kp_id)

    data["updated_at"] = time.time()
    _save(data)
    return kp


def get_wrong_answers(
    learner_id: str,
    kp_id: str = "",
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Get wrong answers for a learner, optionally filtered by kp_id."""
    data = _load(learner_id)
    wrongs = data.get("wrong_answers", [])
    if kp_id:
        wrongs = [w for w in wrongs if w.get("kp_id") == kp_id]
    return wrongs[-limit:]


def weak_points(learner_id: str) -> list[dict[str, Any]]:
    """Get weak knowledge points (level < 0.6)."""
    data = _load(learner_id)
    points = []
    for kp_id, kp in data["mastery"].items():
        if kp["level"] < 0.6 and kp["total"] > 0:
            points.append({
                "kp_id": kp_id,
                "level": kp["level"],
                "total": kp["total"],
            })
    points.sort(key=lambda x: x["level"])
    return points


def get_due_reviews(learner_id: str) -> list[dict[str, Any]]:
    """Get knowledge points due for review (Ebbinghaus spaced repetition)."""
    data = _load(learner_id)
    today = date.today().isoformat()
    due = []
    for kp_id, due_date in data.get("review_schedule", {}).items():
        if due_date <= today:
            kp = data["mastery"].get(kp_id, {"level": 0.0})
            due.append({
                "kp_id": kp_id,
                "level": kp.get("level", 0.0),
                "due_date": due_date,
            })
    due.sort(key=lambda x: x["due_date"])
    return due


def schedule_review(learner_id: str, kp_id: str, level: float) -> None:
    """Schedule a review for a knowledge point based on its mastery level.

    Ebbinghaus intervals: 1d, 3d, 7d, 14d, 30d
    Lower mastery → shorter interval.
    """
    data = _load(learner_id)
    if level < 0.3:
        interval = 1
    elif level < 0.6:
        interval = 3
    elif level < 0.8:
        interval = 7
    elif level < 0.9:
        interval = 14
    else:
        interval = 30

    due = date.today()
    from datetime import timedelta
    due_str = (due + timedelta(days=interval)).isoformat()
    data["review_schedule"][kp_id] = due_str
    data["review_history"].append({
        "kp_id": kp_id,
        "level": level,
        "scheduled": due_str,
        "ts": time.time(),
    })
    data["updated_at"] = time.time()
    _save(data)


def get_answer_history(
    learner_id: str,
    limit: int = 20,
    kp_id: str = "",
) -> list[dict[str, Any]]:
    """Get answer history for a learner."""
    data = _load(learner_id)
    history = data.get("answer_history", [])
    if kp_id:
        history = [h for h in history if h.get("kp_id") == kp_id]
    return history[-limit:]


def get_weekly_stats(learner_id: str) -> dict[str, Any]:
    """Get statistics for the last 7 days."""
    data = _load(learner_id)
    today = date.today()
    from datetime import timedelta
    week_ago = (today - timedelta(days=7)).isoformat()
    stats = {}
    total = correct = wrong = 0
    for day_str, ds in data.get("daily_stats", {}).items():
        if day_str >= week_ago:
            stats[day_str] = ds
            total += ds["total"]
            correct += ds["correct"]
            wrong += ds["wrong"]
    return {
        "daily": stats,
        "total": total,
        "correct": correct,
        "wrong": wrong,
        "accuracy": round(correct / total, 2) if total > 0 else 0,
    }


def get_monthly_stats(learner_id: str) -> dict[str, Any]:
    """Get statistics for the last 30 days."""
    data = _load(learner_id)
    today = date.today()
    from datetime import timedelta
    month_ago = (today - timedelta(days=30)).isoformat()
    stats = {}
    total = correct = wrong = 0
    for day_str, ds in data.get("daily_stats", {}).items():
        if day_str >= month_ago:
            stats[day_str] = ds
            total += ds["total"]
            correct += ds["correct"]
            wrong += ds["wrong"]
    return {
        "daily": stats,
        "total": total,
        "correct": correct,
        "wrong": wrong,
        "accuracy": round(correct / total, 2) if total > 0 else 0,
    }


def generate_daily_report(
    learner_id: str,
    data: dict[str, Any],
) -> dict[str, Any]:
    """Generate a daily learning report from mastery data."""
    today = date.today().isoformat()
    ds = data.get("daily_stats", {}).get(today, {"total": 0, "correct": 0, "wrong": 0, "weak_points": []})

    weak = []
    for wp in ds.get("weak_points", []):
        kp = data["mastery"].get(wp, {})
        weak.append({
            "kp_id": wp,
            "level": kp.get("level", 0),
            "total": kp.get("total", 0),
        })

    return {
        "summary": {
            "total_questions": ds.get("total", 0),
            "correct": ds.get("correct", 0),
            "wrong": ds.get("wrong", 0),
            "accuracy": round(ds.get("correct", 0) / max(ds.get("total", 0), 1), 2),
        },
        "weak_points": weak,
        "learner_id": learner_id,
        "date": today,
    }


def generate_parent_report(learner_id: str, days: int = 7) -> dict[str, Any]:
    """Generate a parent-facing report for daily/weekly/monthly overview."""
    data = _load(learner_id)
    daily = data.get("daily_stats", {})
    answer_history = data.get("answer_history", [])

    # Filter daily_stats by time window
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    recent_days = {k: v for k, v in daily.items() if k >= cutoff}

    total_q = sum(d.get("total", 0) for d in recent_days.values())
    correct_q = sum(d.get("correct", 0) for d in recent_days.values())
    wrong_q = sum(d.get("wrong", 0) for d in recent_days.values())
    weak = set()
    for d in recent_days.values():
        weak.update(d.get("weak_points", []))
    weak_list = sorted(weak)

    # Filter answer_history by time window
    recent_answers = [
        a for a in answer_history
        if isinstance(a.get("timestamp"), str) and a["timestamp"] >= cutoff
    ]

    return {
        "learner_id": learner_id,
        "period_days": days,
        "summary": {
            "total_questions": total_q,
            "correct_count": correct_q,
            "wrong_count": wrong_q,
            "accuracy": round(correct_q / total_q, 2) if total_q > 0 else 0,
        },
        "weak_points": weak_list,
        "recent_wrong": [
            a for a in recent_answers if not a.get("correct", True)
        ][:5],
        "mastery_count": len(data.get("mastery", {})),
    }
