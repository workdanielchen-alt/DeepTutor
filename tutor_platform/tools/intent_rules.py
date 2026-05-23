"""
tutor_platform/tools/intent_rules.py — 设备意图分类规则引擎

基于关键词匹配的设备管理意图分类。
被 mcp_server.py 的 device_command 工具调用。
"""

import logging

logger = logging.getLogger("tutor_platform.tools.intent_rules")

# ── 意图定义 ──
# intent_id: (description, [keywords], min_match)
_INTENTS: dict[str, tuple[str, list[str], int]] = {
    "wifi_connect": (
        "连接WiFi",
        [
            "连接wifi",
            "连wifi",
            "连网",
            "上网设置",
            "配置wifi",
            "wifi设置",
            "网络设置",
            "连无线",
            "设置网络",
            "换wifi",
            "换网络",
            "更换wifi",
            "切换wifi",
            "换一个wifi",
        ],
        1,
    ),
    "wifi_scan": (
        "扫描WiFi",
        [
            "扫描wifi",
            "搜wifi",
            "搜网络",
            "扫描网络",
            "搜无线",
            "wifi列表",
            "可用网络",
            "附近wifi",
            "查看wifi",
        ],
        1,
    ),
    "wifi_status": (
        "查看WiFi状态",
        [
            "wifi状态",
            "网络状态",
            "连接状态",
            "当前网络",
            "wifi信息",
            "ip地址",
            "信号强度",
        ],
        1,
    ),
    "wifi_forget": (
        "忘记WiFi",
        [
            "忘记wifi",
            "删除wifi",
            "移除wifi",
            "取消保存wifi",
            "断开wifi",
            "wifi忘记",
        ],
        1,
    ),
    "device_temp": (
        "查看设备温度",
        [
            "温度",
            "多少度",
            "烫",
            "散热",
            "发热",
            "cpu温度",
            "设备温度",
            "主板温度",
            "温控",
        ],
        1,
    ),
    "device_status": (
        "查看设备状态",
        [
            "设备状态",
            "系统状态",
            "运行状态",
            "设备信息",
            "状态检查",
            "状态查询",
        ],
        1,
    ),
    "storage_info": (
        "查看存储空间",
        [
            "存储",
            "空间",
            "磁盘",
            "硬盘",
            "满了",
            "还剩",
            "存储空间",
            "剩余空间",
            "磁盘空间",
            "内存",
        ],
        1,
    ),
    "ssd_health": (
        "查看SSD健康",
        [
            "ssd",
            "固态",
            "硬盘寿命",
            "磁盘健康",
            "ssd健康",
            "硬盘健康",
            "磨损",
            "寿命",
        ],
        1,
    ),
    "device_alerts": (
        "查看设备告警",
        [
            "告警",
            "报警",
            "异常",
            "故障",
            "问题",
            "设备告警",
            "系统告警",
        ],
        1,
    ),
    "device_cleanup": (
        "清理设备",
        [
            "清理",
            "释放空间",
            "腾空间",
            "清垃圾",
            "设备清理",
            "系统清理",
            "清理缓存",
        ],
        1,
    ),
    "get_bot_qrcode": (
        "获取绑定二维码",
        [
            "加孩子",
            "绑定孩子",
            "子网关",
            "孩子绑定",
            "添加孩子",
            "孩子二维码",
            "绑定二维码",
        ],
        1,
    ),
}

_DEVICE_TOOL_MAP: dict[str, str] = {
    "wifi_connect": "wifi_configure",
    "wifi_scan": "wifi_scan",
    "wifi_status": "wifi_status",
    "wifi_forget": "wifi_forget",
    "device_temp": "device_temp",
    "device_status": "device_status",
    "storage_info": "storage_info",
    "ssd_health": "ssd_health",
    "device_alerts": "device_alerts",
    "device_cleanup": "device_cleanup",
    "get_bot_qrcode": "get_bot_qrcode",
}


def classify_device_intent(text: str) -> dict:
    """对用户文本进行设备管理意图分类。

    Args:
        text: 用户原始消息文本

    Returns:
        {"intent": str, "confidence": float, "description": str}
        intent="none" 表示未匹配
    """
    if not text or not text.strip():
        return {"intent": "none", "confidence": 0.0, "description": ""}

    text_lower = text.lower().strip()

    best_intent = "none"
    best_score = 0.0
    best_desc = ""

    for intent_id, (desc, keywords, min_match) in _INTENTS.items():
        score = 0.0
        matched = 0
        for kw in keywords:
            if kw in text_lower:
                matched += 1
                score += 1.0

        if matched >= min_match and score > best_score:
            best_score = score
            best_intent = intent_id
            best_desc = desc

    # Normalize confidence to [0, 1], cap at 0.95 for keyword match
    confidence = min(best_score / 3.0, 0.95) if best_score > 0 else 0.0

    return {
        "intent": best_intent,
        "confidence": round(confidence, 2),
        "description": best_desc,
    }


def get_device_tool_name(intent: str) -> str:
    """根据意图 ID 获取对应的 MCP 工具名。

    Args:
        intent: 意图 ID (来自 classify_device_intent)

    Returns:
        MCP 工具名称字符串
    """
    return _DEVICE_TOOL_MAP.get(intent, "")
