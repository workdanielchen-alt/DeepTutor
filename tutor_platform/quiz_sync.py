"""
tutor_platform/quiz_sync.py — Quiz → Mastery 同步

将 DeepTutor SQLite 中的答题记录同步到掌握度 JSON 文件。
"""

import json
import logging
import os
import sqlite3
import time

logger = logging.getLogger("tutor_platform.quiz_sync")

DT_DB_PATH = os.environ.get("DEEPTUTOR_DB_PATH", "/data/deeptutor/quiz_records.db")
MASTERY_DIR = os.environ.get("MASTERY_DIR", "/data/mastery")
SYNC_MARKER_DIR = os.environ.get("SYNC_MARKER_DIR", "/data/quiz_sync")


def _get_marker_path(source: str = "deeptutor") -> str:
    """获取同步标记文件路径 (记录已同步的最大 ID)。"""
    os.makedirs(SYNC_MARKER_DIR, exist_ok=True)
    return os.path.join(SYNC_MARKER_DIR, f"{source}_last_id.txt")


def _read_last_sync_id(source: str = "deeptutor") -> int:
    """读取已同步的最大记录 ID。"""
    path = _get_marker_path(source)
    try:
        with open(path) as f:
            return int(f.read().strip())
    except (FileNotFoundError, ValueError):
        return 0


def _write_last_sync_id(sync_id: int, source: str = "deeptutor"):
    """写入已同步的最大记录 ID。"""
    path = _get_marker_path(source)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(str(sync_id))


def _learner_path(learner_id: str) -> str:
    safe = learner_id.replace("/", "_").replace("\\", "_")
    return os.path.join(MASTERY_DIR, f"{safe}.json")


def _load_learner(learner_id: str) -> dict:
    path = _learner_path(learner_id)
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {
            "learner_id": learner_id,
            "mastery": {},
            "wrong_answers": [],
            "total_questions": 0,
            "correct_count": 0,
            "updated_at": time.time(),
        }


def _save_learner(learner_id: str, data: dict):
    data["updated_at"] = time.time()
    path = _learner_path(learner_id)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _has_db() -> bool:
    """检查 SQLite 数据库是否存在。"""
    return os.path.exists(DT_DB_PATH)


def sync_quiz_to_mastery() -> dict:
    """从 DeepTutor SQLite 同步答题记录到掌握度系统。

    Returns:
        {"synced": int, "errors": int, "last_id": int}
    """
    if not _has_db():
        logger.debug("Quiz DB not found at %s, skipping", DT_DB_PATH)
        return {"synced": 0, "errors": 0, "last_id": 0}

    last_id = _read_last_sync_id()
    synced = 0
    errors = 0
    max_id = last_id

    try:
        conn = sqlite3.connect(DT_DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute(
            "SELECT id, learner_id, kp_id, correct, question, user_answer, "
            "correct_answer, ts FROM quiz_records WHERE id > ? ORDER BY id",
            (last_id,),
        )

        for row in cursor.fetchall():
            try:
                record = dict(row)
                learner_id = record.get("learner_id", "default")
                kp_id = record.get("kp_id", "unknown")
                correct = bool(record.get("correct", False))
                question = record.get("question", "") or ""
                user_answer = record.get("user_answer", "") or ""
                correct_answer = record.get("correct_answer", "") or ""

                data = _load_learner(learner_id)
                data["total_questions"] = data.get("total_questions", 0) + 1
                if correct:
                    data["correct_count"] = data.get("correct_count", 0) + 1

                mastery = data.setdefault("mastery", {})
                kp = mastery.setdefault(kp_id, {"level": 0.0, "total": 0, "correct": 0})
                kp["total"] = kp.get("total", 0) + 1
                if correct:
                    kp["correct"] = kp.get("correct", 0) + 1
                kp["level"] = round(kp["correct"] / max(kp["total"], 1), 2)

                if not correct and question:
                    wrongs = data.setdefault("wrong_answers", [])
                    wrongs.append({
                        "kp_id": kp_id,
                        "question": question,
                        "user_answer": user_answer,
                        "correct_answer": correct_answer,
                        "ts": record.get("ts", time.time()),
                    })

                _save_learner(learner_id, data)
                max_id = max(max_id, record["id"])
                synced += 1

            except Exception as e:
                errors += 1
                logger.warning("Sync record #%d error: %s", row.get("id", "?"), e)

        conn.close()

    except sqlite3.Error as e:
        logger.error("Quiz DB connection error: %s", e)
        return {"synced": 0, "errors": 1, "last_id": last_id}

    if synced > 0:
        _write_last_sync_id(max_id)
        logger.info("Quiz sync: %d records synced (errors=%d, last_id=%d)", synced, errors, max_id)

    return {"synced": synced, "errors": errors, "last_id": max_id}
