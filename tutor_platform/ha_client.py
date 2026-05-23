"""
tutor_platform/ha_client.py — Hermes Agent API 客户端

管理 Hermes Agent 的报告定时任务 (日/周/月报) 注册、查询、触发、删除。
"""

import json
import logging
import os

import httpx

logger = logging.getLogger("tutor_platform.ha_client")

HA_URL = os.environ.get("HERMES_AGENT_URL", "http://hermes_agent:8004").rstrip("/")
HA_API_TIMEOUT = 30

# 报告任务定义: (job_name, cron_expr, tool_name, learner_param_optional)
_REPORT_JOBS = [
    {
        "name": "daily_report_push",
        "cron": "30 20 * * *",
        "tool": "push_report",
        "params": {"type": "daily"},
        "description": "每日学习报告推送 (每晚 20:30)",
    },
    {
        "name": "weekly_report_push",
        "cron": "0 10 * * 1",
        "tool": "push_report",
        "params": {"type": "weekly"},
        "description": "每周学习报告推送 (周一 10:00)",
    },
    {
        "name": "monthly_report_push",
        "cron": "0 9 1 * *",
        "tool": "push_report",
        "params": {"type": "monthly"},
        "description": "每月学习报告推送 (每月1日 09:00)",
    },
]


async def ensure_report_jobs() -> list[dict]:
    """注册所有报告定时任务到 Hermes Agent。

    Returns:
        已注册的任务列表
    """
    jobs = []
    for job_def in _REPORT_JOBS:
        try:
            async with httpx.AsyncClient(timeout=HA_API_TIMEOUT) as client:
                resp = await client.post(
                    f"{HA_URL}/api/jobs",
                    json=job_def,
                )
                if resp.status_code in (200, 201):
                    job = resp.json()
                    jobs.append(job)
                    logger.info("HA job registered: %s", job_def["name"])
                elif resp.status_code == 409:
                    logger.debug("HA job already exists: %s", job_def["name"])
                    jobs.append({"name": job_def["name"], "status": "exists"})
                else:
                    logger.warning("HA job registration %s returned %s",
                                   job_def["name"], resp.status_code)
        except Exception as e:
            logger.warning("Failed to register HA job %s: %s", job_def["name"], e)
    return jobs


async def list_jobs() -> list[dict]:
    """列出 Hermes Agent 中所有定时任务。

    Returns:
        任务列表
    """
    try:
        async with httpx.AsyncClient(timeout=HA_API_TIMEOUT) as client:
            resp = await client.get(f"{HA_URL}/api/jobs")
            if resp.status_code == 200:
                return resp.json()
            logger.warning("HA list_jobs returned %s", resp.status_code)
            return []
    except Exception as e:
        logger.warning("Failed to list HA jobs: %s", e)
        return []


async def run_job(job_id: str) -> bool:
    """立即触发执行指定任务。

    Args:
        job_id: 任务 ID

    Returns:
        True 如果触发成功
    """
    try:
        async with httpx.AsyncClient(timeout=HA_API_TIMEOUT) as client:
            resp = await client.post(f"{HA_URL}/api/jobs/{job_id}/run")
            return resp.status_code in (200, 202)
    except Exception as e:
        logger.warning("Failed to run HA job %s: %s", job_id, e)
        return False


async def delete_job(job_id: str) -> bool:
    """删除指定任务。

    Args:
        job_id: 任务 ID

    Returns:
        True 如果删除成功
    """
    try:
        async with httpx.AsyncClient(timeout=HA_API_TIMEOUT) as client:
            resp = await client.delete(f"{HA_URL}/api/jobs/{job_id}")
            return resp.status_code in (200, 204, 404)
    except Exception as e:
        logger.warning("Failed to delete HA job %s: %s", job_id, e)
        return False
