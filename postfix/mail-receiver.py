#!/usr/bin/env python3
"""
mail-receiver.py — Postfix pipe 传输程序（高并发优化版）

调用方式（master.cf 中定义）：
    /usr/local/bin/mail-receiver ${recipient}
    Postfix 通过 stdin 传入完整 RFC 2822 邮件，收件人由 argv[1] 传入。

退出码：
    0  — 成功或静默丢弃（收件人不存在）
    75 — EX_TEMPFAIL，Postfix 放入重试队列（API 不可达时使用）

高并发优化点：
    - stdin 以 binary 模式读取，避免编码解码开销
    - HTTP timeout 缩短为 3s（快速失败，避免进程堆积）
    - 使用 http.client 直连，跳过 urllib 的额外封装层
    - 不解析 JSON 响应体，读完即丢弃，减少 I/O 等待
    - MIME 解析异常直接丢弃（exit 0），不阻塞队列

环境变量：
    API_URL — Go API 内部地址，默认 http://api:8080
"""

import sys
import os
import email
import email.policy
import json
import http.client
import urllib.parse

API_URL = os.environ.get("API_URL", "http://api:8080")


def main():
    if len(sys.argv) < 2:
        sys.exit(1)

    recipient = sys.argv[1].lower().strip()

    # 以 binary 模式读取，避免 Python 默认编码检测开销
    raw_bytes = sys.stdin.buffer.read()
    if not raw_bytes:
        sys.exit(0)

    raw_str = raw_bytes.decode("utf-8", errors="replace")

    # 解析 MIME，出错直接丢弃（exit 0），不阻塞队列
    try:
        msg = email.message_from_string(raw_str, policy=email.policy.default)
    except Exception:
        sys.exit(0)

    sender = str(msg.get("From", ""))
    subject = str(msg.get("Subject", ""))
    body_text = ""
    body_html = ""

    try:
        if msg.is_multipart():
            for part in msg.walk():
                ct = part.get_content_type()
                if ct == "text/plain" and not body_text:
                    body_text = part.get_content()
                elif ct == "text/html" and not body_html:
                    body_html = part.get_content()
        else:
            ct = msg.get_content_type()
            content = msg.get_content()
            if ct == "text/html":
                body_html = content
            else:
                body_text = content
    except Exception:
        pass

    payload = json.dumps(
        {
            "recipient": recipient,
            "sender": sender,
            "subject": subject,
            "body_text": body_text if isinstance(body_text, str) else "",
            "body_html": body_html if isinstance(body_html, str) else "",
            "raw": raw_str,
        },
        ensure_ascii=False,
    ).encode("utf-8")

    # 使用 http.client 直连，3s 超时（快速失败，避免进程堆积）
    parsed = urllib.parse.urlparse(API_URL)
    host = parsed.hostname or "api"
    port = parsed.port or 8080

    try:
        conn = http.client.HTTPConnection(host, port, timeout=3)
        conn.request(
            "POST",
            "/internal/deliver",
            body=payload,
            headers={
                "Content-Type": "application/json",
                "Content-Length": str(len(payload)),
            },
        )
        resp = conn.getresponse()
        resp.read()  # 读完丢弃，释放连接
        conn.close()
        sys.exit(0)
    except Exception as e:
        print(f"deliver error: {e}", file=sys.stderr)
        sys.exit(75)  # EX_TEMPFAIL：放入 Postfix 重试队列


if __name__ == "__main__":
    main()
