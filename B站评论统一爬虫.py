#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
B站评论统一爬虫
支持：视频(BV/AV)、番剧(BV)、动态(opus)
特性：用户筛选、递归子评论、随机延迟、自动重试、导出CSV
"""

import re
import json
import time
import random
import csv
import hashlib
import urllib.parse
from argparse import ArgumentParser
from typing import Tuple, Optional, Dict, Any

import requests
import pandas as pd

# ==================== 配置常量 ====================
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0"
}
WBI_KEY = "ea1db124af3c7062474693fa704f4ff8"
WEB_LOCATION = 1315875
MAX_RETRIES = 3
RETRY_DELAY = 2  # 秒


def get_cookie() -> str:
    """从文件读取Cookie"""
    try:
        with open("bili_cookie.txt", "r", encoding="utf-8") as f:
            return f.read().strip()
    except FileNotFoundError:
        print("[ERROR] 未找到 bili_cookie.txt，请先配置Cookie")
        raise


def get_headers() -> Dict[str, str]:
    """构造请求头"""
    return {
        "Cookie": get_cookie(),
        "User-Agent": HEADERS["User-Agent"],
        "Referer": "https://www.bilibili.com/",
    }


def retry_request(url: str, headers: Dict, params: Optional[Dict] = None, max_retries: int = MAX_RETRIES) -> requests.Response:
    """带重试的请求"""
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=10)
            resp.raise_for_status()
            return resp
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(RETRY_DELAY * (attempt + 1))
    raise RuntimeError(f"请求失败: {url}")


def md5(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def get_aid_and_title_from_av(av_input: str) -> Tuple[str, str]:
    """
    根据AV号（纯数字或带av前缀）获取aid和标题
    返回 (aid, title)
    """
    # 提取纯数字aid
    if av_input.lower().startswith("av"):
        aid = av_input[2:]
    else:
        aid = av_input
    if not aid.isdigit():
        raise ValueError(f"无效的AV号: {av_input}")
    url = f"https://www.bilibili.com/video/av{aid}"
    resp = retry_request(url, headers=get_headers())
    # 提取aid（二次确认）
    aid_match = re.search(r'"aid":(\d+)', resp.text)
    if not aid_match:
        raise ValueError(f"无法从页面提取aid，AV号: {av_input}")
    confirmed_aid = aid_match.group(1)
    # 提取标题
    title_match = re.search(r'<title[^>]*>(.*?)</title>', resp.text)
    title = title_match.group(1) if title_match else "未知视频"
    title = re.sub(r'\s*[-|]\s*哔哩哔哩.*$', '', title)
    return confirmed_aid, title


def get_oid_and_title(content_id: str, content_type: str) -> Tuple[str, str]:
    """
    获取 oid 和标题
    content_type: 'video' / 'bangumi' / 'opus'
    支持 video 的 BV 号或 AV 号（如 av123456 或 123456）
    """
    if content_type == "video":
        # 判断是否为AV号：以av开头或纯数字
        lower_id = content_id.lower()
        if lower_id.startswith("av") or content_id.isdigit():
            return get_aid_and_title_from_av(content_id)
        # 否则按BV处理
        url = f"https://www.bilibili.com/video/{content_id}"
        resp = retry_request(url, headers=get_headers())
        aid_match = re.search(r'"aid":(\d+),"?\s*"bvid":"' + re.escape(content_id) + r'"', resp.text)
        if not aid_match:
            raise ValueError(f"无法从页面提取 aid, BV号: {content_id}")
        oid = aid_match.group(1)
        title_match = re.search(r'<title[^>]*>(.*?)</title>', resp.text)
        title = title_match.group(1) if title_match else "未知视频"
        title = re.sub(r'\s*[-|]\s*哔哩哔哩.*$', '', title)
        return oid, title

    elif content_type == "bangumi":
        url = f"https://www.bilibili.com/bangumi/play/{content_id}"
        resp = retry_request(url, headers=get_headers())
        aid_match = re.search(r'"aid":\s*(\d+)', resp.text)
        if not aid_match:
            raise ValueError(f"无法从番剧页面提取 aid, BV号: {content_id}")
        oid = aid_match.group(1)
        title_match = re.search(r'<title[^>]*>(.*?)</title>', resp.text)
        title = title_match.group(1) if title_match else "未知番剧"
        title = re.sub(r'\s*[-|]\s*哔哩哔哩.*$', '', title)
        return oid, title

    elif content_type == "opus":
        url = f"https://www.bilibili.com/opus/{content_id}"
        resp = retry_request(url, headers=get_headers())
        rid_match = re.search(r'"rid_str":\s*"(\d+)"', resp.text)
        if not rid_match:
            raise ValueError(f"无法从动态页面提取 rid_str, opus: {content_id}")
        oid = rid_match.group(1)
        title_match = re.findall(r'<title>(.+?)</title>', resp.text)
        title = title_match[0].replace("的动态 - 哔哩哔哩", "") if title_match else "未知动态"
        return oid, title

    else:
        raise ValueError(f"不支持的类型: {content_type}")


def build_api_url(oid: str, page_offset: str, mode: int, type_code: int, is_first: bool) -> str:
    """构造带签名的API URL"""
    wts = int(time.time())
    if is_first:
        pagination_str = '{"offset":""}'
        base_params = f"mode={mode}&oid={oid}&pagination_str={urllib.parse.quote(pagination_str)}&plat=1&seek_rpid=&type={type_code}&web_location={WEB_LOCATION}&wts={wts}"
    else:
        pagination_str = f'{{"offset":"{page_offset}"}}'
        base_params = f"mode={mode}&oid={oid}&pagination_str={urllib.parse.quote(pagination_str)}&plat=1&type={type_code}&web_location={WEB_LOCATION}&wts={wts}"
    code = base_params + WBI_KEY
    w_rid = md5(code)
    url = f"https://api.bilibili.com/x/v2/reply/wbi/main?{base_params}&w_rid={w_rid}"
    return url


def extract_comment_info(reply: Dict, is_dynamic: bool = False) -> Dict:
    """从单条回复中提取信息，返回字典"""
    member = reply.get("member", {})
    content = reply.get("content", {})
    reply_control = reply.get("reply_control", {})
    up_action = reply.get("up_action", {})

    info = {
        "parent": reply.get("parent", 0),
        "rpid": reply.get("rpid", 0),
        "uid": reply.get("mid", 0),
        "uname": member.get("uname", ""),
        "message": content.get("message", ""),
        "ctime": pd.to_datetime(reply.get("ctime", 0), unit="s"),
        "like": reply.get("like", 0),
    }

    # 子回复数量
    try:
        sub_text = reply_control.get("sub_reply_entry_text", "0")
        info["sub_reply_count"] = int(re.findall(r"\d+", sub_text)[0])
    except (IndexError, ValueError):
        info["sub_reply_count"] = 0

    # 动态特有字段（视频也提取部分，视频接口也会返回）
    info["level"] = member.get("level_info", {}).get("current_level", 0)
    info["sex"] = member.get("sex", "未知")
    info["avatar"] = member.get("avatar", "")
    info["vip"] = "是" if member.get("vip", {}).get("vipStatus", 0) == 1 else "否"
    info["sign"] = member.get("sign", "")
    try:
        info["ip_location"] = reply_control.get("location", "未知")[5:]  # 去掉 "中国 " 前缀
    except:
        info["ip_location"] = "未知"

    # UP主互动字段（仅视频有效）
    info["up_like"] = "是" if up_action.get("like") else "否"
    info["up_reply"] = "是" if up_action.get("reply") else "否"

    return info


def fetch_sub_comments(oid: str, root_rpid: int, target_uid: str, fetch_all: bool, writer, current_count: int, is_dynamic: bool) -> int:
    """递归爬取指定评论的子评论"""
    page = 1
    while True:
        url = f"https://api.bilibili.com/x/v2/reply/reply?oid={oid}&type={'11' if is_dynamic else '1'}&root={root_rpid}&ps=20&pn={page}&web_location=333.788"
        try:
            resp = retry_request(url, headers=get_headers())
            data = resp.json()
        except Exception as e:
            print(f"  子评论请求失败: {e}")
            break

        replies = data.get("data", {}).get("replies")
        if not replies:
            break

        for rep in replies:
            uid = str(rep.get("mid"))
            if fetch_all or uid == target_uid:
                current_count += 1
                info = extract_comment_info(rep, is_dynamic)
                writer.writerow([current_count] + list(info.values()))
                # 冷却
                if current_count % 500 == 0:
                    sleep_sec = random.uniform(10, 20)
                    print(f"  已爬取 {current_count} 条子评论，暂停 {sleep_sec:.1f}s")
                    time.sleep(sleep_sec)

            # 递归更深层
            sub_cnt = rep.get("reply_control", {}).get("sub_reply_entry_text", "0")
            sub_cnt = int(re.findall(r"\d+", sub_cnt)[0]) if re.findall(r"\d+", sub_cnt) else 0
            if sub_cnt > 0:
                current_count = fetch_sub_comments(oid, rep["rpid"], target_uid, fetch_all, writer, current_count, is_dynamic)

        page += 1
        time.sleep(random.uniform(0.5, 1.5))
    return current_count


def crawl_comments(content_id: str, content_type: str, target_uid: str, enable_sub: bool = True):
    """
    主爬虫入口
    content_type: 'video', 'bangumi', 'opus'
    target_uid: 指定用户ID，'1' 表示爬取全部
    """
    print(f"[INFO] 开始爬取 {content_type} -> {content_id}, 目标UID: {target_uid}")
    oid, title = get_oid_and_title(content_id, content_type)
    print(f"[INFO] oid={oid}, 标题={title}")

    is_dynamic = (content_type == "opus")
    mode = 3 if is_dynamic else 2      # 动态用热门评论(3)，视频用最新(2)
    type_code = 11 if is_dynamic else 1

    fetch_all = (target_uid == "1")
    page_offset = ""
    total_count = 0
    is_first = True

    filename = f"{title[:50]}_{content_id}_{time.strftime('%Y%m%d_%H%M%S')}.csv"
    fieldnames = [
        "序号", "上级评论ID", "评论ID", "用户ID", "用户名", "评论内容", "评论时间",
        "点赞数", "回复数", "用户等级", "性别", "头像", "大会员", "个性签名", "IP属地",
        "UP主点赞", "UP主回复"
    ]

    with open(filename, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(fieldnames)

        while True:
            url = build_api_url(oid, page_offset, mode, type_code, is_first)
            try:
                resp = retry_request(url, headers=get_headers())
                data = resp.json()
            except Exception as e:
                print(f"[ERROR] 请求第 {page_offset or '1'} 页失败: {e}")
                break

            replies = data.get("data", {}).get("replies")
            if not replies:
                print("[INFO] 本页无评论，爬取结束")
                break

            for rep in replies:
                uid = str(rep.get("mid"))
                if fetch_all or uid == target_uid:
                    total_count += 1
                    info = extract_comment_info(rep, is_dynamic)
                    writer.writerow([total_count] + list(info.values()))

                    # 每1000条主评论冷却
                    if total_count % 1000 == 0:
                        sleep_sec = random.uniform(30, 60)
                        print(f"[COOLDOWN] 已爬取 {total_count} 条，暂停 {sleep_sec:.1f}s")
                        time.sleep(sleep_sec)

                    # 子评论
                    if enable_sub and info["sub_reply_count"] > 0:
                        print(f"  爬取评论 {info['rpid']} 的 {info['sub_reply_count']} 条子评论...")
                        total_count = fetch_sub_comments(
                            oid, info["rpid"], target_uid, fetch_all, writer, total_count, is_dynamic
                        )
                else:
                    # 即使不是目标用户，如果启用子评论且该评论有子评论，仍需深入检查
                    if enable_sub:
                        sub_cnt = rep.get("reply_control", {}).get("sub_reply_entry_text", "0")
                        sub_cnt = int(re.findall(r"\d+", sub_cnt)[0]) if re.findall(r"\d+", sub_cnt) else 0
                        if sub_cnt > 0:
                            total_count = fetch_sub_comments(
                                oid, rep["rpid"], target_uid, fetch_all, writer, total_count, is_dynamic
                            )

            # 翻页
            cursor = data.get("data", {}).get("cursor", {})
            pagination = cursor.get("pagination_reply", {})
            next_offset = pagination.get("next_offset")
            if not next_offset:
                break

            page_offset = str(next_offset)
            is_first = False
            delay = random.uniform(1.0, 3.0)
            print(f"[PAGE] 下一页 offset={page_offset}, 暂停 {delay:.1f}s")
            time.sleep(delay)

    print(f"[SUCCESS] 爬取完成！共 {total_count} 条评论，已保存至 {filename}")


def main():
    parser = ArgumentParser(description="B站评论爬虫（统一版）")
    parser.add_argument("--id", required=True, help="视频BV号/AV号（如av123或123456） / 番剧BV号 / 动态opus号")
    parser.add_argument("--type", choices=["video", "bangumi", "opus"], default="video", help="内容类型")
    parser.add_argument("--uid", default="1", help="指定用户ID，'1'表示爬取全部")
    parser.add_argument("--no-sub", action="store_false", dest="sub", help="不爬取子评论")
    args = parser.parse_args()

    crawl_comments(args.id, args.type, args.uid, args.sub)


if __name__ == "__main__":
    main()