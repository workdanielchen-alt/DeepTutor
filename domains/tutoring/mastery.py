"""
domains/tutoring/mastery.py — 掌握度追踪与分析 (v7.0)

使用 JSON 文件存储学习者的掌握度数据和错题本。
"""

import json
import os
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger("domains.tutoring.mastery")

MASTERY_DIR = os.environ.get("MASTERY_DIR", "/data/mastery")


def _ensure_dir():
    os.makedirs(MASTERY_DIR, exist_ok=True)


def _learner_path(learner_id: str) -> str:
    safe = learner_id.replace("/", "_").replace("\\", "_")
    return os.path.join(MASTERY_DIR, f"{safe}.json")


def _migrate_v1(data: dict) -> dict:
    """迁移 v0 → v1: 添加 daily_stats, answer_history, review_schedule, review_history。"""
    if "daily_stats" not in data:
        data["daily_stats"] = {}
    if "answer_history" not in data:
        data["answer_history"] = []
    if "review_schedule" not in data:
        data["review_schedule"] = {}
    if "review_history" not in data:
        data["review_history"] = []
    if "version" not in data:
        data["version"] = 1
    return data


def _load(learner_id: str) -> dict:
    """加载学习者的全部掌握度数据。"""
    p = _learner_path(learner_id)
    try:
        with open(p) as f:
            data = json.load(f)
        return _migrate_v1(data)
    except (FileNotFoundError, json.JSONDecodeError):
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


def _save(learner_id: str, data: dict):
    _ensure_dir()
    data["updated_at"] = time.time()
    p = _learner_path(learner_id)
    with open(p, "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_mastery(learner_id: str, kp_id: str = "") -> dict:
    """获取学习者在某个知识点的掌握度。"""
    data = _load(learner_id)
    if kp_id:
        mastery = data.get("mastery", {}).get(kp_id, {"level": 0, "total": 0, "correct": 0})
        return {
            "kp_id": kp_id,
            "level": mastery.get("level", 0),
            "total": mastery.get("total", 0),
            "correct": mastery.get("correct", 0),
            "accuracy": round(mastery.get("correct", 0) / max(mastery.get("total", 1), 1) * 100, 1),
        }
    return {
        "total_questions": data.get("total_questions", 0),
        "accuracy": round(data.get("correct_count", 0) / max(data.get("total_questions", 1), 1) * 100, 1),
        "mastery_count": len(data.get("mastery", {})),
    }


def get_mastery_summary(learner_id: str) -> dict:
    """获取掌握度概要。"""
    data = _load(learner_id)
    mastery = data.get("mastery", {})
    weak = {k: v for k, v in mastery.items() if v.get("level", 0) < 0.6}
    return {
        "total_kp": len(mastery),
        "mastered": sum(1 for v in mastery.values() if v.get("level", 0) >= 0.8),
        "learning": sum(1 for v in mastery.values() if 0.6 <= v.get("level", 0) < 0.8),
        "weak": len(weak),
        "weak_kps": list(weak.keys()),
        "total_questions": data.get("total_questions", 0),
        "accuracy": round(data.get("correct_count", 0) / max(data.get("total_questions", 1), 1) * 100, 1),
    }


def get_wrong_answers(learner_id: str, kp_id: str = "", limit: int = 10) -> list[dict]:
    """获取学习者的错题列表。"""
    data = _load(learner_id)
    wrongs = data.get("wrong_answers", [])
    if kp_id:
        wrongs = [w for w in wrongs if w.get("kp_id") == kp_id]
    return sorted(wrongs, key=lambda x: x.get("ts", 0), reverse=True)[:limit]


def update_mastery(learner_id: str, kp_id: str, correct: bool, question: str = "",
                   user_answer: str = "", correct_answer: str = ""):
    """更新掌握度和记录答题结果。"""
    data = _load(learner_id)
    ts = time.time()
    data["total_questions"] = data.get("total_questions", 0) + 1
    if correct:
        data["correct_count"] = data.get("correct_count", 0) + 1

    # Update per-kp mastery
    mastery = data.setdefault("mastery", {})
    kp = mastery.setdefault(kp_id, {"level": 0.0, "total": 0, "correct": 0})
    kp["total"] = kp.get("total", 0) + 1
    if correct:
        kp["correct"] = kp.get("correct", 0) + 1
    kp["level"] = round(kp["correct"] / max(kp["total"], 1), 2)

    # Record wrong answer (keep last 50)
    if not correct and question:
        wrongs = data.setdefault("wrong_answers", [])
        wrongs.append({
            "kp_id": kp_id,
            "question": question,
            "user_answer": user_answer,
            "correct_answer": correct_answer,
            "ts": ts,
        })
        if len(wrongs) > 50:
            wrongs[:] = wrongs[-50:]

    # Record all attempts in answer_history
    answer_history = data.setdefault("answer_history", [])
    answer_history.append({
        "kp_id": kp_id,
        "question": question,
        "user_answer": user_answer,
        "correct_answer": correct_answer,
        "is_correct": correct,
        "ts": ts,
    })
    if len(answer_history) > 500:
        answer_history[:] = answer_history[-500:]

    # Update daily_stats
    today = datetime.now().strftime("%Y-%m-%d")
    daily_stats = data.setdefault("daily_stats", {})
    day = daily_stats.setdefault(today, {"total": 0, "correct": 0, "wrong": 0})
    day["total"] = day.get("total", 0) + 1
    if correct:
        day["correct"] = day.get("correct", 0) + 1
    else:
        day["wrong"] = day.get("wrong", 0) + 1

    # Track weak KPIs per day
    if not correct:
        day_weak = set(day.get("weak_points", []))
        day_weak.add(kp_id)
        day["weak_points"] = sorted(day_weak)

    _save(learner_id, data)


def get_daily_stats(learner_id: str, date_str: str = "") -> dict:
    """获取某天的学习统计。"""
    data = _load(learner_id)
    if not date_str:
        date_str = datetime.now().strftime("%Y-%m-%d")
    return data.get("daily_stats", {}).get(date_str, {"total": 0, "correct": 0, "wrong": 0})


def get_period_stats(learner_id: str, days: int) -> dict:
    """获取最近 N 天的聚合统计。"""
    data = _load(learner_id)
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    daily_stats = data.get("daily_stats", {})
    total = correct = wrong = 0
    day_stats = []
    weak_kps = set()
    for d_str in sorted(daily_stats.keys()):
        if d_str >= cutoff:
            s = daily_stats[d_str]
            total += s.get("total", 0)
            correct += s.get("correct", 0)
            wrong += s.get("wrong", 0)
            day_stats.append({"date": d_str, **s})
            for wp in s.get("weak_points", []):
                weak_kps.add(wp)
    return {
        "days": days,
        "total": total,
        "correct": correct,
        "wrong": wrong,
        "accuracy": round(correct / max(total, 1) * 100, 1),
        "per_day": day_stats,
        "weak_points": sorted(weak_kps),
    }


def get_weekly_stats(learner_id: str) -> dict:
    """获取最近 7 天统计。"""
    return get_period_stats(learner_id, 7)


def get_monthly_stats(learner_id: str) -> dict:
    """获取最近 30 天统计。"""
    return get_period_stats(learner_id, 30)


def get_answer_history(learner_id: str, limit: int = 20, kp_id: str = "") -> list[dict]:
    """获取答题历史。"""
    data = _load(learner_id)
    history = data.get("answer_history", [])
    if kp_id:
        history = [h for h in history if h.get("kp_id") == kp_id]
    return sorted(history, key=lambda x: x.get("ts", 0), reverse=True)[:limit]


# Ebbinghaus review intervals based on mastery level
_REVIEW_INTERVALS = {
    "weak": 1,        # level < 0.4: next day
    "learning": 3,    # level 0.4–0.6: 3 days
    "improving": 7,   # level 0.6–0.8: 7 days
    "mastered": 14,   # level >= 0.8: 14 days
    "consolidated": 30,  # level >= 0.9: 30 days
}


def _review_interval(level: float) -> int:
    """根据掌握度返回 Ebbinghaus 复习间隔天数。"""
    if level >= 0.9:
        return _REVIEW_INTERVALS["consolidated"]
    if level >= 0.8:
        return _REVIEW_INTERVALS["mastered"]
    if level >= 0.6:
        return _REVIEW_INTERVALS["improving"]
    if level >= 0.4:
        return _REVIEW_INTERVALS["learning"]
    return _REVIEW_INTERVALS["weak"]


def schedule_review(learner_id: str, kp_id: str, level: float | None = None):
    """为知识点安排下一次复习时间 (Ebbinghaus 间隔)。"""
    data = _load(learner_id)
    if level is None:
        kp = data.get("mastery", {}).get(kp_id, {})
        level = kp.get("level", 0)
    interval_days = _review_interval(level)
    next_review = (datetime.now() + timedelta(days=interval_days)).strftime("%Y-%m-%d")
    schedule = data.setdefault("review_schedule", {})
    schedule[kp_id] = next_review
    _save(learner_id, data)


def get_due_reviews(learner_id: str) -> list[dict]:
    """获取今天到期的复习知识点列表。"""
    data = _load(learner_id)
    today = datetime.now().strftime("%Y-%m-%d")
    schedule = data.get("review_schedule", {})
    mastery = data.get("mastery", {})
    due = []
    for kp_id, due_date in schedule.items():
        if due_date <= today:
            kp = mastery.get(kp_id, {})
            due.append({
                "kp_id": kp_id,
                "level": kp.get("level", 0),
                "total": kp.get("total", 0),
                "correct": kp.get("correct", 0),
                "due_date": due_date,
            })
    return sorted(due, key=lambda x: x["due_date"])


def record_review(learner_id: str, kp_id: str, correct: bool):
    """记录一次复习结果并安排下一次复习。"""
    data = _load(learner_id)
    # Update mastery based on review
    mastery = data.setdefault("mastery", {})
    kp = mastery.setdefault(kp_id, {"level": 0.0, "total": 0, "correct": 0})
    kp["total"] = kp.get("total", 0) + 1
    if correct:
        kp["correct"] = kp.get("correct", 0) + 1
    kp["level"] = round(kp["correct"] / max(kp["total"], 1), 2)
    # Record review
    review_history = data.setdefault("review_history", [])
    review_history.append({
        "kp_id": kp_id,
        "correct": correct,
        "level_after": kp["level"],
        "ts": time.time(),
    })
    if len(review_history) > 200:
        review_history[:] = review_history[-200:]
    # Schedule next review
    schedule_review(learner_id, kp_id, kp["level"])
    _save(learner_id, data)


def weak_points(learner_id: str) -> list[dict]:
    """获取薄弱知识点列表 (掌握度 < 0.6)。"""
    data = _load(learner_id)
    weak = []
    for kp_id, kp in data.get("mastery", {}).items():
        if kp.get("level", 0) < 0.6:
            weak.append({
                "kp_id": kp_id,
                "level": kp.get("level", 0),
                "total": kp.get("total", 0),
                "correct": kp.get("correct", 0),
            })
    return sorted(weak, key=lambda x: x["level"])


def get_report(learner_id: str) -> dict:
    """获取学习者的完整报告。"""
    data = _load(learner_id)
    mastery = data.get("mastery", {})
    return {
        "learner_id": learner_id,
        "total_questions": data.get("total_questions", 0),
        "correct_count": data.get("correct_count", 0),
        "accuracy": round(data.get("correct_count", 0) / max(data.get("total_questions", 1), 1) * 100, 1),
        "mastery_count": len(mastery),
        "weak_points": weak_points(learner_id),
        "recent_wrong": get_wrong_answers(learner_id, limit=5),
    }


def generate_daily_report(learner_id: str, data: dict = None) -> dict:
    """生成日度学习报告。"""
    if data is None:
        data = _load(learner_id)
    mastery = data.get("mastery", {})
    wrongs = data.get("wrong_answers", [])
    today = time.time() - 86400
    today_wrongs = [w for w in wrongs if w.get("ts", 0) > today]
    return {
        "summary": {
            "total_questions": data.get("total_questions", 0),
            "correct_count": data.get("correct_count", 0),
            "accuracy": round(data.get("correct_count", 0) / max(data.get("total_questions", 1), 1) * 100, 1),
            "total_kp": len(mastery),
            "weak_kp": len([v for v in mastery.values() if v.get("level", 0) < 0.6]),
            "today_wrong": len(today_wrongs),
        },
        "weak_points": weak_points(learner_id),
        "recent_wrong": today_wrongs[:5],
    }


def generate_parent_report(learner_id: str, days: int | None = None) -> dict:
    """生成面向家长的完整学习报告。

    Args:
        learner_id: 学习者 ID
        days: 统计天数 (7=周报, 30=月报, None=全部)
    """
    data = _load(learner_id)
    mastery = data.get("mastery", {})
    wrongs = data.get("wrong_answers", [])

    if days is not None:
        period_stats = get_period_stats(learner_id, days)
        cutoff_ts = time.time() - days * 86400
        period_wrongs = [w for w in wrongs if w.get("ts", 0) > cutoff_ts]
        # Weak points within period
        period_weak = weak_points(learner_id)
        period_weak = [w for w in period_weak if w["kp_id"] in period_stats["weak_points"]]
        return {
            "learner_id": learner_id,
            "period": f"last_{days}d",
            "period_stats": period_stats,
            "summary": {
                "total_questions": period_stats["total"],
                "correct_count": period_stats["correct"],
                "accuracy": period_stats["accuracy"],
                "weak_kp": len(period_weak),
                "total_kp": len(mastery),
            },
            "weak_points": period_weak,
            "recent_wrong": sorted(period_wrongs, key=lambda x: x.get("ts", 0), reverse=True)[:10],
        }

    # Full report (no time filter)
    weak = weak_points(learner_id)
    return {
        "learner_id": learner_id,
        "period": "all",
        "summary": {
            "total_questions": data.get("total_questions", 0),
            "correct_count": data.get("correct_count", 0),
            "accuracy": round(data.get("correct_count", 0) / max(data.get("total_questions", 1), 1) * 100, 1),
            "mastered_kp": sum(1 for v in mastery.values() if v.get("level", 0) >= 0.8),
            "weak_kp": len(weak),
            "total_kp": len(mastery),
        },
        "weak_points": weak,
        "recent_wrong": sorted(wrongs, key=lambda x: x.get("ts", 0), reverse=True)[:10],
    }
