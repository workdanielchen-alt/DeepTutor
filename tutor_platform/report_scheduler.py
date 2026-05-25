"""
tutor_platform/report_scheduler.py — 学习报告调度与推送

生成日报/周报/月报并写入通知文件，由 Hermes Agent 消费后推送到微信。
"""

import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path

logger = logging.getLogger("tutor_platform.report_scheduler")

MASTERY_DIR = os.environ.get("MASTERY_DIR", "/data/mastery")
NOTIFICATION_DIR = "/data/hermes/notifications"


def enumerate_learners() -> list[str]:
    """扫描掌握度目录，返回所有学习者 ID 列表。"""
    master_dir = Path(MASTERY_DIR)
    if not master_dir.exists():
        return []
    learners = []
    for f in master_dir.iterdir():
        if f.suffix == ".json":
            learner_id = f.stem
            if learner_id:
                learners.append(learner_id)
    return sorted(learners)


def _write_notification(learner_id: str, report_type: str, content: str, target: str = "parent") -> bool:
    """写一条报告推送通知到共享目录，供 Hermes Agent 消费。

    Args:
        learner_id: 学习者标识
        report_type: 报告类型 ("daily" / "weekly" / "monthly" / "exam")
        content: 推送文本
        target: 目标网关 ("parent" → 家长, "child" → 孩子)
    """
    try:
        notif_dir = Path(NOTIFICATION_DIR)
        notif_dir.mkdir(parents=True, exist_ok=True)
        notification = {
            "type": "report_push",
            "learner_id": learner_id,
            "report_type": report_type,
            "content": content,
            "target": target,
            "timestamp": time.time(),
        }
        notif_path = notif_dir / f"report_{learner_id}_{int(time.time())}.json"
        tmp_path = notif_path.with_suffix(".tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(notification, f, ensure_ascii=False)
        os.replace(tmp_path, notif_path)
        logger.info("Report notification written: %s", notif_path)
        return True
    except Exception as e:
        logger.warning("Failed to write report notification: %s", e)
        return False


async def push_daily_reports() -> list[dict]:
    """为所有学习者推送日报。"""
    from domains.tutoring.mastery import generate_parent_report
    from tutor_platform.report_push import format_parent_report_for_wechat

    learners = enumerate_learners()
    results = []
    for learner_id in learners:
        try:
            report = generate_parent_report(learner_id, days=1)
            if report["summary"]["total_questions"] == 0:
                continue
            text = format_parent_report_for_wechat(learner_id, report)
            ok = _write_notification(learner_id, "daily", text)
            results.append({"learner_id": learner_id, "ok": ok})
        except Exception as e:
            logger.error("Daily report failed for %s: %s", learner_id, e)
            results.append({"learner_id": learner_id, "ok": False, "error": str(e)})
    return results


async def push_weekly_reports() -> list[dict]:
    """为所有学习者推送周报。"""
    from domains.tutoring.mastery import generate_parent_report
    from tutor_platform.report_push import format_parent_report_for_wechat

    learners = enumerate_learners()
    results = []
    for learner_id in learners:
        try:
            report = generate_parent_report(learner_id, days=7)
            if report["summary"]["total_questions"] == 0:
                continue
            text = format_parent_report_for_wechat(learner_id, report)
            ok = _write_notification(learner_id, "weekly", text)
            results.append({"learner_id": learner_id, "ok": ok})
        except Exception as e:
            logger.error("Weekly report failed for %s: %s", learner_id, e)
            results.append({"learner_id": learner_id, "ok": False, "error": str(e)})
    return results


async def push_monthly_reports() -> list[dict]:
    """为所有学习者推送月报。"""
    from domains.tutoring.mastery import generate_parent_report
    from tutor_platform.report_push import format_monthly_report_text

    learners = enumerate_learners()
    results = []
    for learner_id in learners:
        try:
            report = generate_parent_report(learner_id, days=30)
            if report["summary"]["total_questions"] == 0:
                continue
            text = format_monthly_report_text(learner_id, report)
            ok = _write_notification(learner_id, "monthly", text)
            results.append({"learner_id": learner_id, "ok": ok})
        except Exception as e:
            logger.error("Monthly report failed for %s: %s", learner_id, e)
            results.append({"learner_id": learner_id, "ok": False, "error": str(e)})
    return results
