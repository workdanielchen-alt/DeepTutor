"""
tutor_platform/report_push.py — 学习报告格式化与推送

提供日报/周报/月报的格式化文本生成。
由 provider_api.py 的 /api/report/generate 端点使用。
"""

import logging
from datetime import datetime

logger = logging.getLogger("tutor_platform.report_push")


def format_daily_report(report: dict) -> str:
    """格式化每日学习报告为可读文本。

    Args:
        report: generate_daily_report() 的输出

    Returns:
        格式化后的报告文本 (空结果表示当日无学习)
    """
    summary = report.get("summary", {})
    total = summary.get("total_questions", 0)

    if total == 0:
        return ""

    today = datetime.now().strftime("%Y-%m-%d")
    accuracy = summary.get("accuracy", 0)
    weak_kp = summary.get("weak_kp", 0)
    today_wrong = summary.get("today_wrong", 0)
    total_kp = summary.get("total_kp", 0)

    lines = [
        f"📚 学习日报 ({today})",
        "─" * 20,
        f"累计答题: {total} 题",
        f"正确率: {accuracy}%",
        f"知识点覆盖: {total_kp} 个",
        f"薄弱点: {weak_kp} 个",
    ]

    if today_wrong > 0:
        lines.append(f"今日错题: {today_wrong} 道")
        lines.append("")
        lines.append("📝 今日错题:")
        for i, w in enumerate(report.get("recent_wrong", []), 1):
            q = w.get("question", "")[:60]
            correct = w.get("correct_answer", "")
            lines.append(f"  {i}. {q}")
            lines.append(f"     正确答案: {correct}")

    lines.append("")
    lines.append("💪 继续加油！")

    return "\n".join(lines)


def _format_period_label(report: dict) -> str:
    """根据报告周期返回中文标签。"""
    period = report.get("period", "all")
    if period == "last_7d":
        return "本周"
    if period == "last_30d":
        return "本月"
    return "累计"


def format_parent_report_for_wechat(learner_id: str, report: dict) -> str:
    """格式化面向家长的完整学习报告 (微信友好)。

    Args:
        learner_id: 学习者标识
        report: generate_parent_report() 的输出

    Returns:
        WeChat 友好格式的报告文本
    """
    summary = report.get("summary", {})
    total = summary.get("total_questions", 0)
    period_label = _format_period_label(report)

    if period_label == "累计":
        title = f"📊 学习报告 — {learner_id}"
    else:
        title = f"📊 {period_label}学习报告 — {learner_id}"

    lines = [title, "=" * 20]

    if total == 0:
        lines.append(f"{period_label}暂无学习记录")
        return "\n".join(lines)

    accuracy = summary.get("accuracy", 0)
    correct = summary.get("correct_count", 0)
    weak = summary.get("weak_kp", 0)
    total_kp = summary.get("total_kp", 0)

    lines.append(f"📝 {period_label}学习概况")
    lines.append(f"  答题: {total} 题 (正确 {correct} 题)")
    lines.append(f"  正确率: {accuracy}%")
    lines.append("")

    # Per-day breakdown (weekly/monthly reports)
    period_stats = report.get("period_stats")
    if period_stats and period_stats.get("per_day"):
        lines.append(f"📅 每日详情:")
        for day in period_stats["per_day"]:
            d = day.get("date", "")[-5:]  # "MM-DD"
            d_total = day.get("total", 0)
            d_correct = day.get("correct", 0)
            d_wrong = day.get("wrong", 0)
            if d_total > 0:
                d_acc = round(d_correct / d_total * 100, 1)
                marks = "✅" if d_acc >= 80 else ("📖" if d_acc >= 60 else "⚠️")
                lines.append(f"  {marks} {d}: {d_total}题 {d_acc}%")
            else:
                lines.append(f"  {d}: 未学习")
        lines.append("")

    if weak > 0:
        lines.append("⚠️ 薄弱知识点:")
        for w in report.get("weak_points", []):
            kp = w.get("kp_id", "未知")
            level = w.get("level", 0) * 100
            lines.append(f"  • {kp} (掌握度 {level:.0f}%)")
        lines.append("")

    wrong_list = report.get("recent_wrong", [])
    if wrong_list:
        lines.append(f"❌ 最近错题 ({min(len(wrong_list), 10)} 道):")
        for i, w in enumerate(wrong_list[:10], 1):
            q = w.get("question", "")[:80]
            answer = w.get("correct_answer", "")
            lines.append(f"  {i}. {q}")
            if answer:
                lines.append(f"     → {answer[:60]}")
        lines.append("")

    # Trend indicator
    if period_stats and period_stats.get("accuracy", 0) >= 80:
        lines.append("⭐ 正确率优秀，继续保持！")
    elif period_stats and period_stats.get("accuracy", 0) >= 60:
        lines.append("💪 进步空间很大，加油！")
    else:
        lines.append("🎯 需要加强基础练习，家长多鼓励孩子！")

    return "\n".join(lines)


def format_monthly_report_text(learner_id: str, report: dict) -> str:
    """格式化月度学习报告。

    Args:
        learner_id: 学习者标识
        report: generate_parent_report(learner_id, days=30) 的输出

    Returns:
        格式化月度报告文本
    """
    summary = report.get("summary", {})
    total = summary.get("total_questions", 0)

    today = datetime.now()
    month = today.strftime("%Y年%m月")

    if total == 0:
        return f"📊 {month}学习报告 — {learner_id}\n\n本月暂无学习记录"

    accuracy = summary.get("accuracy", 0)
    correct = summary.get("correct_count", 0)
    weak = summary.get("weak_kp", 0)
    total_kp = summary.get("total_kp", 0)

    lines = [
        f"📊 {month}学习报告 — {learner_id}",
        "=" * 25,
        "",
        f"📝 本月学习概况",
        f"  答题总数: {total} 题",
        f"  正确数量: {correct} 题",
        f"  正确率: {accuracy}%",
    ]

    # Per-day breakdown
    period_stats = report.get("period_stats")
    if period_stats and period_stats.get("per_day"):
        active_days = sum(1 for d in period_stats["per_day"] if d.get("total", 0) > 0)
        lines.append(f"  学习天数: {active_days} 天")
        lines.append("")
        lines.append(f"📅 每日趋势:")
        for day in period_stats["per_day"]:
            d = day.get("date", "")[-5:]
            d_total = day.get("total", 0)
            if d_total > 0:
                d_acc = round(day.get("correct", 0) / d_total * 100, 1)
                bar = "█" * max(1, int(d_acc / 10))
                lines.append(f"  {d}: {bar} {d_acc}% ({d_total}题)")
            else:
                lines.append(f"  {d}: -")

    lines.append("")
    lines.append(f"📖 知识掌握")
    lines.append(f"  已覆盖知识点: {total_kp} 个")

    if weak > 0:
        lines.append("")
        lines.append("⚠️ 薄弱知识点 (需重点关注):")
        for w in report.get("weak_points", []):
            kp = w.get("kp_id", "未知")
            level = w.get("level", 0) * 100
            total_q = w.get("total", 0)
            lines.append(f"  • {kp}: 掌握度 {level:.0f}% (练习 {total_q} 题)")

    # Wrong answers this month
    wrong_list = report.get("recent_wrong", [])
    if wrong_list:
        lines.append("")
        lines.append(f"❌ 本月错题 ({len(wrong_list)} 道):")
        for i, w in enumerate(wrong_list[:8], 1):
            q = w.get("question", "")[:60]
            lines.append(f"  {i}. {q}")

    lines.append("")
    lines.append("🎯 下月建议")
    if weak > 0:
        lines.append("  • 针对薄弱知识点加强练习")
    if accuracy < 70:
        lines.append("  • 建议回顾错题, 巩固基础")
    else:
        lines.append("  • 继续保持, 挑战更高难度")
    lines.append("  • 坚持每日练习, 积少成多")

    return "\n".join(lines)
