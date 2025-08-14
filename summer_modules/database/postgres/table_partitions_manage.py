import psycopg2
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import re
import matplotlib.pyplot as plt
from pathlib import Path
from matplotlib.font_manager import FontProperties as FP

from summer_modules.database.postgres import POSTGRES_LOGGER
from summer_modules.markdown import Markdown
from summer_modules.utils import find_chinese_font


# 创建标准分区（过去12个月+当前月）
def create_standard_partitions(
    conn_string: str, months_back: int = 12, custom_logger=None
) -> dict:
    """
    创建过去指定月数和当前月的IOC表分区

    Args:
        conn_string: 数据库连接字符串
        months_back: 要创建的过去月份数量, 默认12个月
        custom_logger: 自定义日志记录器, 默认为 None

    Returns:
        dict: 包含分区创建结果的统计信息
    """
    if not custom_logger:
        custom_logger = POSTGRES_LOGGER

    stats = {
        "created_partitions": [],
        "existing_partitions": [],
        "errors": [],
        "total_created": 0,
    }

    try:
        with psycopg2.connect(conn_string) as conn:
            with conn.cursor() as cur:
                # 查询现有分区
                cur.execute(
                    """
                    SELECT relname 
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relkind = 'r' 
                      AND n.nspname = 'public'
                      AND c.relname LIKE 'ioc_info_%'
                """
                )
                existing_partitions = [row[0] for row in cur.fetchall()]

                # 计算需要的分区范围
                now = datetime.now()
                current_month = now.replace(
                    day=1, hour=0, minute=0, second=0, microsecond=0
                )

                # 计算开始月份（当前月往前推X个月）
                if current_month.month <= months_back % 12:
                    start_year = current_month.year - (months_back // 12 + 1)
                    start_month = current_month.month + 12 - (months_back % 12)
                else:
                    start_year = current_month.year - (months_back // 12)
                    start_month = current_month.month - (months_back % 12)

                start_date = datetime(start_year, start_month, 1)
                custom_logger.info(
                    f"将创建从 {start_date.strftime('%Y-%m-%d')} 到 {current_month.strftime('%Y-%m-%d')} 的分区"
                )

                # 生成所有需要的月份
                required_partitions = []
                current_date = start_date

                while current_date <= current_month:
                    partition_name = (
                        f"ioc_info_{current_date.year}_{current_date.month:02d}"
                    )

                    # 计算下个月（分区结束日期）
                    if current_date.month == 12:
                        next_month = current_date.replace(
                            year=current_date.year + 1, month=1
                        )
                    else:
                        next_month = current_date.replace(month=current_date.month + 1)

                    required_partitions.append(
                        {
                            "name": partition_name,
                            "start_date": current_date,
                            "end_date": next_month,
                            "exists": partition_name in existing_partitions,
                        }
                    )

                    current_date = next_month

                # 创建缺失的分区
                for partition_info in required_partitions:
                    partition_name = partition_info["name"]

                    if partition_info["exists"]:
                        custom_logger.info(f"分区 {partition_name} 已存在，跳过创建")
                        stats["existing_partitions"].append(partition_name)
                        continue

                    partition_start = partition_info["start_date"].strftime("%Y-%m-%d")
                    partition_end = partition_info["end_date"].strftime("%Y-%m-%d")

                    try:
                        # 创建分区
                        cur.execute(
                            f"""
                            CREATE TABLE IF NOT EXISTS {partition_name} PARTITION OF ioc_info
                            FOR VALUES FROM ('{partition_start}') TO ('{partition_end}');
                        """
                        )

                        # 为分区创建所需的索引
                        cur.execute(
                            f"""
                            CREATE INDEX IF NOT EXISTS idx_{partition_name}_indicator ON {partition_name}(indicator);
                            CREATE INDEX IF NOT EXISTS idx_{partition_name}_type ON {partition_name}(type);
                            CREATE INDEX IF NOT EXISTS idx_{partition_name}_pulse_id ON {partition_name}(pulse_id);
                            CREATE INDEX IF NOT EXISTS idx_{partition_name}_created ON {partition_name}(created);
                        """
                        )

                        conn.commit()
                        custom_logger.info(
                            f"成功创建分区 {partition_name}，日期范围: {partition_start} 至 {partition_end}"
                        )
                        stats["created_partitions"].append(
                            {
                                "name": partition_name,
                                "start_date": partition_start,
                                "end_date": partition_end,
                            }
                        )
                        stats["total_created"] += 1

                    except Exception as e:
                        conn.rollback()
                        error_msg = f"创建分区 {partition_name} 失败: {str(e)}"
                        custom_logger.error(error_msg)
                        stats["errors"].append(error_msg)

        return stats

    except Exception as e:
        error_msg = f"创建标准分区时出错: {str(e)}"
        custom_logger.error(error_msg)
        stats["errors"].append(error_msg)
        return stats


# 检查并创建未来分区
def check_and_create_future_partitions(
    conn_string: str, months_ahead: int = 3, custom_logger=None
) -> dict:
    """
    检查并创建未来几个月的IOC表分区, 如果所有分区都已存在则直接返回

    Args:
        conn_string: 数据库连接字符串
        months_ahead: 提前创建多少个月的分区
        custom_logger: 自定义日志记录器, 默认为 None

    Returns:
        dict: 包含创建结果的统计信息
    """
    if not custom_logger:
        custom_logger = POSTGRES_LOGGER

    stats = {
        "created_partitions": [],
        "existing_partitions": [],
        "errors": [],
        "total_created": 0,
    }

    try:
        # 连接数据库
        with psycopg2.connect(conn_string) as conn:
            with conn.cursor() as cur:
                # 查询现有分区
                cur.execute(
                    """
                    SELECT relname 
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relkind = 'r' 
                      AND n.nspname = 'public'
                      AND c.relname LIKE 'ioc_info_%'
                """
                )
                existing_partitions = [row[0] for row in cur.fetchall()]

                # 计算需要创建的月份
                now = datetime.now()
                current_month = now.replace(
                    day=1, hour=0, minute=0, second=0, microsecond=0
                )

                # 先检查所有未来月份的分区是否都已存在
                required_partitions = []
                for i in range(1, months_ahead + 1):
                    # 计算下一个月
                    if current_month.month == 12:
                        next_month = current_month.replace(
                            year=current_month.year + 1, month=1
                        )
                    else:
                        next_month = current_month.replace(
                            month=current_month.month + 1
                        )

                    # 构建分区名称
                    partition_name = (
                        f"ioc_info_{next_month.year}_{next_month.month:02d}"
                    )

                    # 添加到需要检查的分区列表
                    required_partitions.append(
                        {
                            "name": partition_name,
                            "month": next_month,
                            "exists": partition_name in existing_partitions,
                        }
                    )

                    # 更新当前月份
                    current_month = next_month

                # 检查是否所有需要的分区都已存在
                all_partitions_exist = all(p["exists"] for p in required_partitions)
                if all_partitions_exist:
                    custom_logger.info(
                        f"所有未来{months_ahead}个月的分区已存在，无需创建"
                    )
                    for p in required_partitions:
                        stats["existing_partitions"].append(p["name"])
                    return stats

                # 否则，创建不存在的分区
                for partition_info in required_partitions:
                    partition_name = partition_info["name"]
                    next_month = partition_info["month"]

                    if partition_info["exists"]:
                        custom_logger.info(f"分区 {partition_name} 已存在，跳过创建")
                        stats["existing_partitions"].append(partition_name)
                        continue

                    # 计算下下个月（分区结束日期）
                    if next_month.month == 12:
                        after_next_month = next_month.replace(
                            year=next_month.year + 1, month=1
                        )
                    else:
                        after_next_month = next_month.replace(
                            month=next_month.month + 1
                        )

                    partition_start = next_month.strftime("%Y-%m-%d")
                    partition_end = after_next_month.strftime("%Y-%m-%d")

                    # 创建新分区
                    try:
                        # 为月份创建分区
                        cur.execute(
                            f"""
                            CREATE TABLE IF NOT EXISTS {partition_name} PARTITION OF ioc_info
                            FOR VALUES FROM ('{partition_start}') TO ('{partition_end}');
                        """
                        )

                        # 为分区创建所需的索引
                        cur.execute(
                            f"""
                            CREATE INDEX IF NOT EXISTS idx_{partition_name}_indicator ON {partition_name}(indicator);
                            CREATE INDEX IF NOT EXISTS idx_{partition_name}_type ON {partition_name}(type);
                            CREATE INDEX IF NOT EXISTS idx_{partition_name}_pulse_id ON {partition_name}(pulse_id);
                            CREATE INDEX IF NOT EXISTS idx_{partition_name}_created ON {partition_name}(created);
                        """
                        )

                        conn.commit()
                        custom_logger.info(
                            f"成功创建分区 {partition_name}，日期范围: {partition_start} 至 {partition_end}"
                        )
                        stats["created_partitions"].append(
                            {
                                "name": partition_name,
                                "start_date": partition_start,
                                "end_date": partition_end,
                            }
                        )
                        stats["total_created"] += 1

                    except Exception as e:
                        conn.rollback()
                        error_msg = f"创建分区 {partition_name} 失败: {str(e)}"
                        custom_logger.error(error_msg)
                        stats["errors"].append(error_msg)

        return stats

    except Exception as e:
        custom_logger.error(f"连接数据库或创建分区时出错: {str(e)}")
        stats["errors"].append(f"整体错误: {str(e)}")
        return stats


# 维护当前活跃分区
def maintain_active_partitions(
    conn_string: str, active_months: int = 3, custom_logger=None
) -> dict:
    """
    对当前活跃的分区执行维护操作, 包括VACUUM ANALYZE和索引重建

    Args:
        conn_string: 数据库连接字符串
        active_months: 最近几个月的分区视为活跃分区
        custom_logger: 自定义日志记录器, 用于记录日志信息

    Returns:
        dict: 包含维护结果的统计信息
    """
    if not custom_logger:
        custom_logger = POSTGRES_LOGGER

    stats = {
        "maintained_partitions": [],
        "vacuum_results": {},
        "reindex_results": {},
        "errors": [],
    }

    try:
        # 分两步处理：先获取分区信息
        active_partition_names = []
        # 连接数据库
        with psycopg2.connect(conn_string) as conn:
            with conn.cursor() as cur:
                # 查询现有分区
                cur.execute(
                    """
                    SELECT relname, pg_size_pretty(pg_total_relation_size(c.oid)) as size,
                           pg_stat_get_numscans(c.oid) as scans
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relkind = 'r' 
                      AND n.nspname = 'public'
                      AND c.relname LIKE 'ioc_info_%'
                    ORDER BY relname DESC
                """
                )
                all_partitions = cur.fetchall()

                # 计算活跃分区日期范围
                now = datetime.now()
                current_month = now.replace(
                    day=1, hour=0, minute=0, second=0, microsecond=0
                )

                # 找出活跃分区
                active_partition_prefixes = []
                for i in range(active_months):
                    month = current_month.month - i
                    year = current_month.year
                    while month <= 0:
                        month += 12
                        year -= 1
                    active_partition_prefixes.append(f"ioc_info_{year}_{month:02d}")

                # 仅识别活跃分区，不执行VACUUM
                for partition_info in all_partitions:
                    partition_name = partition_info[0]
                    partition_size = partition_info[1]
                    scan_count = partition_info[2]

                    # 检查是否为活跃分区
                    is_active = any(
                        partition_name.startswith(prefix)
                        for prefix in active_partition_prefixes
                    )
                    if is_active:
                        active_partition_names.append(
                            {
                                "name": partition_name,
                                "size": partition_size,
                                "scan_count": scan_count,
                            }
                        )

        # 第二步：为VACUUM创建手动管理的连接
        vacuum_conn = None
        vacuum_cur = None
        try:
            # 创建连接并设置autocommit
            vacuum_conn = psycopg2.connect(conn_string)
            vacuum_conn.autocommit = True  # 在事务外执行命令
            vacuum_cur = vacuum_conn.cursor()

            # 执行分区维护
            for partition in active_partition_names:
                partition_name = partition["name"]

                custom_logger.info(
                    f"维护活跃分区: {partition_name} (大小: {partition['size']}, 扫描次数: {partition['scan_count']})"
                )

                try:
                    # 执行VACUUM ANALYZE
                    vacuum_cur.execute(f"VACUUM ANALYZE {partition_name};")
                    stats["vacuum_results"][partition_name] = "成功"

                    # 检查索引碎片并根据需要重建
                    vacuum_cur.execute(
                        f"""
                        SELECT indexrelname, pg_size_pretty(pg_relation_size(indexrelid)) as size,
                               idx_scan, idx_tup_read, idx_tup_fetch
                        FROM pg_stat_user_indexes
                        WHERE relname = '{partition_name}'
                    """
                    )
                    indexes = vacuum_cur.fetchall()

                    reindex_results = []
                    for idx in indexes:
                        index_name = idx[0]
                        index_size = idx[1]
                        idx_scan = idx[2]

                        # 如果索引有大量扫描，考虑重建
                        if idx_scan > 100:  # 可调整的阈值
                            vacuum_cur.execute(f"REINDEX INDEX {index_name};")
                            reindex_results.append(f"{index_name} (大小: {index_size})")

                    if reindex_results:
                        stats["reindex_results"][partition_name] = reindex_results

                    stats["maintained_partitions"].append(partition)

                except Exception as e:
                    error_msg = f"维护分区 {partition_name} 失败: {str(e)}"
                    custom_logger.error(error_msg)
                    stats["errors"].append(error_msg)

        finally:
            # 确保资源被正确释放
            if vacuum_cur is not None:
                vacuum_cur.close()
            if vacuum_conn is not None:
                vacuum_conn.close()

        return stats

    except Exception as e:
        custom_logger.error(f"连接数据库或维护分区时出错: {str(e)}")
        stats["errors"].append(f"整体错误: {str(e)}")
        return stats


def calculate_partition_time_range(partition_stats: dict) -> dict:
    """
    从分区名称计算时间范围

    Args:
        partition_stats: 分区统计信息

    Returns:
        dict: 包含最早和最晚日期的字典
    """
    earliest = None
    latest = None

    # 获取所有分区名称
    all_partitions = partition_stats.get("existing_partitions", []) + [
        p["name"] for p in partition_stats.get("created_partitions", [])
    ]

    # 从分区名称提取年月
    dates = []
    for partition in all_partitions:
        match = re.match(r"ioc_info_(\d{4})_(\d{2})", partition)
        if match:
            year = int(match.group(1))
            month = int(match.group(2))
            # 创建日期对象
            partition_date = datetime(year, month, 1)
            dates.append(partition_date)

    # 如果有日期，计算最早和最晚
    if dates:
        earliest = min(dates).strftime("%Y-%m-%d")
        latest = max(dates).strftime("%Y-%m-%d")

    return {"earliest": earliest, "latest": latest}


# 根据各任务的执行结果创建综合报告
def create_partition_management_report(
    partition_stats: dict,
    maintenance_stats: dict,
    pie_chart_ioc_partition_distribution_filepath: Path,
    standard_partition_stats: dict | None = None,
    custom_logger=None,
    archive_stats: dict | None = None,
) -> str:
    """
    根据各任务的执行结果创建综合报告

    Args:
        partition_stats: 未来分区创建统计
        maintenance_stats: 分区维护统计
        standard_partition_stats: 标准分区创建统计（可选）
        custom_logger: 自定义日志记录器（可选）
        archive_stats: 分区归档统计(可选):暂时没有归档需求, 遇到需求再写

    Returns:
        str: Markdown格式的报告内容

    """
    if not custom_logger:
        custom_logger = POSTGRES_LOGGER
    # 创建Markdown报告
    report_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    markdown_report = Markdown()
    markdown_report.clear_all()
    markdown_report.add_header(header=f"IOC数据库分区管理报告 - {report_time}", level=1)
    if standard_partition_stats:
        # 从分区名称计算时间范围
        time_range = calculate_partition_time_range(standard_partition_stats)
        earliest = time_range.get("earliest", "未知")
        latest = time_range.get("latest", "未知")
        markdown_report.add_header(header="1. 标准分区摘要", level=2)
        markdown_report.add_list(
            items=[
                f"**分区时间范围**: {earliest} 至 {latest}",
                f"**创建的新分区**: {standard_partition_stats.get('total_created', 0)}个",
                f"**已存在的分区**: {len(standard_partition_stats.get('existing_partitions', []))}个",
                f"**错误数**: {len(standard_partition_stats.get('errors', []))}个",
            ]
        )
        markdown_report.add_header(header="新创建的标准分区", level=3)
        markdown_report.add_table(
            headers=["分区名", "起始日期", "结束日期"],
            rows=[
                (partition["name"], partition["start_date"], partition["end_date"])
                for partition in standard_partition_stats.get("created_partitions", [])
            ],
        )

    markdown_report.add_header(
        header=f"{2 if standard_partition_stats else 1}. 未来分区创建摘要", level=2
    )
    markdown_report.add_list(
        items=[
            f"**创建的新分区**: {partition_stats.get('total_created', 0)}个",
            f"**已存在的分区**: {len(partition_stats.get('existing_partitions', []))}个",
            f"**错误数**: {len(partition_stats.get('errors', []))}个",
        ]
    )
    markdown_report.add_header(header="新创建的未来分区", level=3)
    markdown_report.add_table(
        headers=["分区名", "起始日期", "结束日期"],
        rows=[
            (partition["name"], partition["start_date"], partition["end_date"])
            for partition in partition_stats.get("created_partitions", [])
        ],
    )

    markdown_report.add_header(
        header=f"{3 if standard_partition_stats else 2}. 分区维护摘要", level=2
    )
    markdown_report.add_list(
        items=[
            f"**维护的分区数**: {len(maintenance_stats.get('maintained_partitions', []))}个",
            f"**执行VACUUM的分区**: {len(maintenance_stats.get('vacuum_results', {}))}个",
            f"**索引重建操作**: {sum(len(v) for v in maintenance_stats.get('reindex_results', {}).values())}个",
            f"**错误数**: {len(maintenance_stats.get('errors', []))}个",
        ]
    )
    markdown_report.add_header(header="维护的分区详情", level=3)
    markdown_report.add_table(
        headers=["分区名", "大小", "扫描次数", "VACUUM结果", "重建的索引数"],
        rows=[
            (
                partition["name"],
                partition["size"],
                partition["scan_count"],
                maintenance_stats["vacuum_results"].get(partition["name"], "未执行"),
                len(maintenance_stats["reindex_results"].get(partition["name"], [])),
            )
            for partition in maintenance_stats.get("maintained_partitions", [])
        ],
    )

    if archive_stats:
        markdown_report.add_header(
            header=f"{4 if standard_partition_stats else 3}. 分区归档摘要", level=2
        )
        markdown_report.add_list(
            items=[
                f"**归档的分区数**: {len(archive_stats.get('archived_partitions', []))}个",
                f"**移动到归档表空间**: {len(archive_stats.get('tablespace_moves', []))}个",
                f"**错误数**: {len(archive_stats.get('errors', []))}个",
            ]
        )
        markdown_report.add_header(header="归档的分区详情", level=3)
        markdown_report.add_table(
            headers=["分区名", "年份", "月份", "大小", "操作"],
            rows=[
                (
                    partition["name"],
                    partition["year"],
                    partition["month"],
                    partition["size"],
                    "移至归档表空间" if partition["moved"] else "仅标记归档",
                )
                for partition in archive_stats.get("archived_partitions", [])
            ],
        )

    # 添加错误信息
    all_errors = (
        (standard_partition_stats.get("errors", []) if standard_partition_stats else [])
        + partition_stats.get("errors", [])
        + maintenance_stats.get("errors", [])
    )
    if archive_stats:
        all_errors += archive_stats.get("errors", [])

    if all_errors:
        all_error_markdown_level = 3
        all_error_markdown_level += 1 if standard_partition_stats else 0
        all_error_markdown_level += 1 if archive_stats else 0
        markdown_report.add_header(
            header=f"{all_error_markdown_level}. 错误摘要", level=2
        )
        for i, error in enumerate(all_errors):
            markdown_report.add_paragraph(f"**错误 {i + 1}**: {error}")

    # 创建分区状态可视化
    try:
        # 收集分区数据
        current_partitions = partition_stats.get("existing_partitions", []) + [
            p["name"] for p in partition_stats.get("created_partitions", [])
        ]

        standard_partitions = []
        if standard_partition_stats:
            standard_partitions = standard_partition_stats.get(
                "existing_partitions", []
            ) + [
                p["name"]
                for p in standard_partition_stats.get("created_partitions", [])
            ]
            current_partitions.extend(standard_partitions)

        if archive_stats:
            archived_partitions = [
                p["name"] for p in archive_stats.get("archived_partitions", [])
            ]
        else:
            archived_partitions = []

        maintained_partitions = [
            p["name"] for p in maintenance_stats.get("maintained_partitions", [])
        ]

        # 去重
        current_partitions = list(set(current_partitions))

        chinese_font = find_chinese_font()
        if chinese_font:
            font = FP(fname=chinese_font, size=12)
            plt.rcParams["font.family"] = font.get_name()
        else:
            custom_logger.warning("没有找到中文字体,可能会导致中文显示不正常")

        # 准备图表数据
        if standard_partition_stats:
            labels = ["未来分区", "标准分区", "归档分区", "维护分区"]
            sizes = [
                len(current_partitions) - len(standard_partitions),
                len(standard_partitions),
                len(archived_partitions),
                len(maintained_partitions),
            ]
        else:
            labels = ["当前分区", "归档分区", "维护分区"]
            sizes = [
                len(current_partitions),
                len(archived_partitions),
                len(maintained_partitions),
            ]

        # 数据验证 - 防止NaN错误
        if sum(sizes) > 0:
            # 创建饼图
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.pie(sizes, labels=labels, autopct="%1.1f%%", startangle=90)
            ax.set_title("IOC分区状态分布")
            plt.axis("equal")

            # 将图表转换为图像
            fig.savefig(
                pie_chart_ioc_partition_distribution_filepath, bbox_inches="tight"
            )

            custom_logger.info(
                f"IOC分区管理报告已创建，共处理 {len(current_partitions)} 个分区"
            )
        else:
            custom_logger.warning("没有足够的分区数据用于创建可视化")

    except Exception as e:
        custom_logger.error(f"创建分区状态可视化失败: {str(e)}")

    return markdown_report.content
