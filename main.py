from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, register
from astrbot.api import logger, AstrBotConfig
from typing import Optional, List, Dict, Any
import asyncio
import time
import random
from datetime import datetime, date

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from .bottle_storage import BottleStorage
from .cloud_bottle_storage import CloudBottleStorage
from .image_handler import ImageHandler
from .config_manager import ConfigManager
from .message_formatter import MessageFormatter
from .uploaded_bottles_tracker import UploadedBottlesTracker

class DriftBottlePlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config_manager = ConfigManager(config)  # 先初始化配置管理器
        self.storage = BottleStorage("data")
        self.cloud_storage = CloudBottleStorage(self.config_manager)  # 传入配置管理器
        self.image_handler = ImageHandler()
        self.message_formatter = MessageFormatter()
        self.upload_tracker = UploadedBottlesTracker("data")
        self.sync_task = None

        # 群通知频率控制状态（内存中记录，重启后重置）
        self._notify_last_time: float = 0.0           # 上次通知的时间戳
        self._notify_today_count: int = 0             # 今日已通知次数
        self._notify_today_date: date = date.today()  # 当前统计日期，跨天时重置计数

        # 定时通知调度器
        self.cron_scheduler: Optional[AsyncIOScheduler] = None

        # 如果启用了云同步，启动定时同步任务
        if self.config_manager.is_cloud_sync_enabled():
            self.start_sync_task()

        # 如果启用了定时群通知，启动调度器
        if self.config_manager.is_group_notify_cron_enabled():
            self.start_cron_notify()

    def start_sync_task(self):
        """启动定时同步任务"""
        if self.sync_task is None:
            self.sync_task = asyncio.create_task(self._sync_loop())
            logger.info("已启动云同步任务")

    def stop_sync_task(self):
        """停止定时同步任务"""
        if self.sync_task:
            self.sync_task.cancel()
            self.sync_task = None
            logger.info("已停止云同步任务")

    async def _sync_loop(self):
        """定时同步循环"""
        while True:
            try:
                await self._sync_bottles()
                await asyncio.sleep(self.config_manager.get_cloud_sync_interval())
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"云同步任务出错: {str(e)}")
                await asyncio.sleep(60)  # 出错后等待1分钟再重试

    async def _sync_bottles(self):
        """同步本地漂流瓶到云端"""
        try:
            bottles = self.storage.get_bottles_to_upload()  # 获取需要上传的漂流瓶
            batch_size = self.config_manager.get_cloud_sync_batch_size()
            uploaded_count = 0
            
            for bottle in bottles:
                if uploaded_count >= batch_size:
                    break
                    
                try:
                    bottle_id = await self.cloud_storage.add_bottle(
                        content=bottle["content"],
                        images=bottle["images"],
                        sender=bottle["sender"],
                        sender_id=bottle["sender_id"]
                    )
                    
                    if isinstance(bottle_id, int):
                        self.storage.mark_uploaded(bottle["id"])  # 标记已上传
                        uploaded_count += 1
                        logger.info(f"成功上传漂流瓶 {bottle['id']} 到云端")
                    elif isinstance(bottle_id, dict) and "error" in bottle_id:
                        logger.warning(f"上传漂流瓶 {bottle['id']} 失败: {bottle_id['error']}")
                    
                    # 遵守速率限制
                    await asyncio.sleep(12)  # 每5个/分钟 = 每12秒1个
                except Exception as e:
                    logger.error(f"上传漂流瓶 {bottle['id']} 时出错: {str(e)}")
            
            if uploaded_count > 0:
                logger.info(f"本次同步共上传了 {uploaded_count} 个漂流瓶")
        except Exception as e:
            logger.error(f"执行同步任务时出错: {str(e)}")

    def _check_notify_frequency(self) -> bool:
        """检查是否满足群通知频率限制条件，满足返回True，否则返回False"""
        now = time.time()
        today = date.today()

        # 跨天重置计数
        if today != self._notify_today_date:
            self._notify_today_date = today
            self._notify_today_count = 0

        # 检查每日次数限制（0表示不限制）
        daily_limit = self.config_manager.get_group_notify_daily_limit()
        if daily_limit > 0 and self._notify_today_count >= daily_limit:
            logger.info(
                f"今日群通知次数已达上限 {daily_limit} 次，跳过本次通知"
            )
            return False

        # 检查最小间隔时间（0表示不限制）
        min_interval = self.config_manager.get_group_notify_min_interval()
        if min_interval > 0 and self._notify_last_time > 0:
            elapsed_minutes = (now - self._notify_last_time) / 60
            if elapsed_minutes < min_interval:
                logger.info(
                    f"距离上次群通知不足 {min_interval} 分钟（已过 {elapsed_minutes:.1f} 分钟），跳过本次通知"
                )
                return False

        return True

    def _get_bot(self) -> Optional[Any]:
        """从平台管理器获取第一个 aiocqhttp 类型的 bot 实例

        用于在无 event 的场景下（如 cron 定时任务）调用 QQ API。
        """
        try:
            for platform in self.context.platform_manager.platform_insts:
                # aiocqhttp 适配器有 bot 属性
                bot = getattr(platform, "bot", None)
                if bot is not None:
                    return bot
        except Exception as e:
            logger.error(f"获取 bot 实例失败: {str(e)}")
        return None

    async def _send_to_random_group(
        self, bot: Any, notify_message: str, exclude_group_id: Optional[int] = None
    ) -> bool:
        """向随机群发送通知消息（核心发送逻辑，不依赖 event）

        Args:
            bot: aiocqhttp 的 bot 实例
            notify_message: 要发送的通知文本
            exclude_group_id: 需要排除的群 ID（如扔漂流瓶所在的群），避免在原群通知

        Returns:
            bool: 是否发送成功
        """
        try:
            # 获取群列表
            # 注意：aiocqhttp 的 call_action 返回的已经是 data 字段的内容（群列表数组），
            # 而非完整的响应 dict。这里兼容两种返回格式。
            group_list_result = await bot.call_action("get_group_list")

            # 兼容处理：返回值可能是 list（标准 aiocqhttp）或 dict（含 data 字段）
            if isinstance(group_list_result, list):
                groups = group_list_result
            elif isinstance(group_list_result, dict):
                groups = group_list_result.get("data", [])
            else:
                logger.warning(f"获取群列表返回数据格式异常: {type(group_list_result)}")
                return False

            if not groups:
                logger.info("机器人没有加入任何群，无法发送通知")
                return False

            # 排除指定群（如扔漂流瓶所在的群），避免在原群通知
            if exclude_group_id is not None:
                before_count = len(groups)
                groups = [g for g in groups if g.get("group_id") != exclude_group_id]
                logger.info(f"已排除扔漂流瓶所在群 {exclude_group_id}，剩余候选群 {len(groups)}/{before_count} 个")
                if not groups:
                    logger.info("排除原群后没有其他可用群，无法发送通知")
                    return False

            # 最大重试次数（换群重试）
            max_retries = self.config_manager.get_group_notify_max_retries()
            # 记录已尝试过的群，避免重复选中同一个不可用的群
            tried_group_ids: set = set()

            # 总尝试次数 = 初始1次 + 重试次数
            for attempt in range(max_retries + 1):
                # 过滤掉已尝试失败的群
                available_groups = [
                    g for g in groups if g.get("group_id") not in tried_group_ids
                ]
                if not available_groups:
                    logger.warning("所有可用群均已尝试失败，无可发送通知的群")
                    return False

                # 随机选择一个群
                target_group = random.choice(available_groups)
                group_id = target_group.get("group_id")
                group_name = target_group.get("group_name", "未知群")
                logger.info(f"当前漂流瓶尝试通知目标群: {group_name}({group_id})")

                if not group_id:
                    logger.warning("选中的群没有有效的 group_id")
                    tried_group_ids.add(group_id)
                    continue

                try:
                    # 发送群消息
                    await bot.send_group_msg(
                        group_id=group_id,
                        message=[{"type": "text", "data": {"text": notify_message}}]
                    )

                    # 发送成功，更新频率控制状态
                    self._notify_last_time = time.time()
                    self._notify_today_count += 1

                    logger.info(
                        f"已向群 {group_name}({group_id}) 发送漂流瓶通知"
                        f"（今日第 {self._notify_today_count} 次"
                        f"{f'，重试 {attempt} 次后成功' if attempt > 0 else ''}）"
                    )
                    return True

                except Exception as send_err:
                    # 发送失败（如退群、被禁言），记录并换群重试
                    tried_group_ids.add(group_id)
                    logger.warning(
                        f"向群 {group_name}({group_id}) 发送通知失败: {str(send_err)}"
                    )
                    if attempt < max_retries:
                        logger.info(f"将重新选择其他群重试（第 {attempt + 1}/{max_retries} 次）")
                    else:
                        logger.error(
                            f"已达最大重试次数 {max_retries}，本次群通知发送失败"
                        )

        except Exception as e:
            logger.error(f"发送群通知失败: {str(e)}")
        return False

    async def _send_group_notification(self, event: AstrMessageEvent):
        """发送漂流瓶群通知（由扔漂流瓶触发）

        会自动排除扔漂流瓶所在的群，避免在原群通知。
        """
        # 检查是否启用群通知
        if not self.config_manager.is_group_notify_enabled():
            return

        # 检查频率限制
        if not self._check_notify_frequency():
            return

        # 获取通知消息内容
        notify_message = self.config_manager.get_group_notify_message()

        # 优先使用 event.bot，兜底用 _get_bot()
        bot = getattr(event, "bot", None) or self._get_bot()
        if bot is None:
            logger.warning("无法获取 bot 实例，跳过群通知")
            return

        # 获取扔漂流瓶所在的群 ID，用于排除该群
        exclude_group_id = None
        try:
            # aiocqhttp 群消息事件的 raw_message 中包含 group_id
            raw_message = getattr(event.message_obj, "raw_message", None)
            if isinstance(raw_message, dict):
                exclude_group_id = raw_message.get("group_id")
        except Exception:
            pass

        await self._send_to_random_group(bot, notify_message, exclude_group_id)

    def _parse_cron_expression(self, cron_expr: str) -> dict:
        """解析5字段Cron表达式为 APScheduler CronTrigger 参数

        格式：分 时 日 月 周
        例如 '0 10,14,18,22 * * *' 表示每天10点、14点、18点、22点
        """
        fields = cron_expr.strip().split()
        if len(fields) != 5:
            raise ValueError(
                f"Cron表达式格式错误，需要5个字段（分 时 日 月 周），当前有{len(fields)}个字段: {cron_expr}"
            )
        return {
            "minute": fields[0],
            "hour": fields[1],
            "day": fields[2],
            "month": fields[3],
            "day_of_week": fields[4],
        }

    def start_cron_notify(self):
        """启动定时群通知调度器"""
        try:
            cron_expr = self.config_manager.get_group_notify_cron_expression()
            trigger_args = self._parse_cron_expression(cron_expr)

            self.cron_scheduler = AsyncIOScheduler()
            self.cron_scheduler.add_job(
                self._cron_notify_callback,
                trigger=CronTrigger(**trigger_args),
                id="drift_bottle_cron_notify",
                misfire_grace_time=60,
            )
            self.cron_scheduler.start()
            logger.info(f"已启动定时群通知调度器，Cron表达式: {cron_expr}")
        except Exception as e:
            logger.error(f"启动定时群通知调度器失败: {str(e)}")

    def stop_cron_notify(self):
        """停止定时群通知调度器"""
        if self.cron_scheduler:
            self.cron_scheduler.shutdown(wait=False)
            self.cron_scheduler = None
            logger.info("已停止定时群通知调度器")

    async def _cron_notify_callback(self):
        """定时群通知回调：检查是否有未捡起的漂流瓶，有则发通知"""
        try:
            # 检查是否有未捡起的漂流瓶
            active_count, _ = self.storage.get_bottle_counts()
            if active_count <= 0:
                logger.info("定时群通知检查：当前没有未捡起的漂流瓶，跳过通知")
                return

            # 检查频率限制
            if not self._check_notify_frequency():
                return

            # 获取 bot 实例
            bot = self._get_bot()
            if bot is None:
                logger.warning("定时群通知：无法获取 bot 实例，跳过通知")
                return

            # 构造通知消息（包含未捡起数量）
            base_message = self.config_manager.get_group_notify_message()
            notify_message = f"🌊 当前海面上还有 {active_count} 个漂流瓶等待被捡起！\n{base_message}"

            logger.info(f"定时群通知触发：有 {active_count} 个未捡起的漂流瓶")
            await self._send_to_random_group(bot, notify_message)

        except Exception as e:
            logger.error(f"定时群通知回调出错: {str(e)}")

    @filter.command("扔漂流瓶")
    async def throw_bottle(self, event: AstrMessageEvent, content: str = ""):
        """扔一个漂流瓶"""
        # 收集所有图片
        images = await self.image_handler.collect_images(event)
        
        # 如果既没有文字内容也没有图片，则返回错误提示
        if not content and not images:
            yield event.plain_result("漂流瓶不能是空的哦，请至少包含文字或图片～")
            return

        # 检查内容限制
        passed, error_msg = self.config_manager.check_content_limits(content, images)
        if not passed:
            yield event.plain_result(error_msg)
            return

        # 只保留允许的最大图片数量
        max_images = self.config_manager.get_value("max_images")
        images = images[:max_images]
        
        # 添加漂流瓶
        bottle_id = self.storage.add_bottle(
            content=content,
            images=images,
            sender=event.get_sender_name(),
            sender_id=event.get_sender_id()
        )
        
        # 发送群通知（异步执行，不阻塞用户响应）
        asyncio.create_task(self._send_group_notification(event))
        
        yield event.plain_result(f"你的漂流瓶已经扔进大海了！瓶子的编号是 {bottle_id}")

    @filter.command("捡漂流瓶")
    async def pick_bottle(self, event: AstrMessageEvent):
        """捡起一个漂流瓶"""
        bottle = self.storage.pick_random_bottle()
        if not bottle:
            yield event.plain_result("海面上没有漂流瓶了...")
            return

        yield self.message_formatter.create_bottle_message(event, bottle, "你捡到了一个漂流瓶！")

    @filter.command("被捡起的漂流瓶")
    async def picked_bottle(self, event: AstrMessageEvent, bottle_id: Optional[int] = None):
        """查看已捡起的漂流瓶"""
        bottle = self.storage.get_picked_bottle(bottle_id)
        if not bottle:
            if bottle_id is not None:
                yield event.plain_result(f"没有找到编号为 {bottle_id} 的漂流瓶")
            else:
                yield event.plain_result("还没有被捡起的漂流瓶...")
            return

        yield self.message_formatter.create_bottle_message(event, bottle, "这是一个被捡起的漂流瓶！")

    @filter.command("未被捡起的漂流瓶")
    async def bottle_count(self, event: AstrMessageEvent):
        """查看当前漂流瓶数量"""
        active_count, picked_count = self.storage.get_bottle_counts()
        yield event.plain_result(
            f"当前海面上还有 {active_count} 个漂流瓶\n"
            f"已经被捡起的漂流瓶有 {picked_count} 个"
        )

    @filter.command("被捡起的漂流瓶列表")
    async def list_picked_bottles(self, event: AstrMessageEvent):
        """显示所有被捡起的漂流瓶列表"""
        bottles = self.storage.get_picked_bottles()
        message = self.message_formatter.format_picked_bottles_list(bottles)
        yield event.plain_result(message)

    @filter.command("扔云漂流瓶")
    async def throw_cloud_bottle(self, event: AstrMessageEvent, content: str = ""):
        """扔一个云漂流瓶"""
        # 收集所有图片
        images = await self.image_handler.collect_images(event)
        
        # 如果既没有文字内容也没有图片，则返回错误提示
        if not content and not images:
            yield event.plain_result("漂流瓶不能是空的哦，请至少包含文字或图片～")
            return

        # 检查内容限制
        passed, error_msg = self.config_manager.check_content_limits(content, images)
        if not passed:
            yield event.plain_result(error_msg)
            return

        # 只保留允许的最大图片数量
        max_images = self.config_manager.get_value("max_images")
        images = images[:max_images]
        
        # 添加云漂流瓶
        try:
            result = await self.cloud_storage.add_bottle(
                content=content,
                images=images,
                sender=event.get_sender_name(),
                sender_id=event.get_sender_id()
            )
            if isinstance(result, dict) and "error" in result:
                yield event.plain_result(result["error"])
            elif result:
                yield event.plain_result(f"你的云漂流瓶已经扔进云端大海了！瓶子的编号是 {result}")
            else:
                yield event.plain_result("抱歉，扔云漂流瓶失败了，请稍后再试...")
        except Exception as e:
            logger.error(f"Failed to throw cloud bottle: {e}")
            yield event.plain_result("抱歉，扔云漂流瓶时遇到了问题，请稍后再试...")

    @filter.command("捡云漂流瓶")
    async def pick_cloud_bottle(self, event: AstrMessageEvent):
        """捡起一个云漂流瓶"""
        try:
            result = await self.cloud_storage.pick_random_bottle(event.get_sender_id())
            if not result:
                yield event.plain_result("云端海面上没有漂流瓶了...")
                return

            if "error" in result:
                yield event.plain_result(result["error"])
                return

            bottle = result["bottle"]
            is_reset = result["is_reset"]

            # 如果是重置后的瓶子，添加提示信息
            prefix_message = "你从云端捡到了一个漂流瓶！"
            if is_reset:
                prefix_message = "云端的新漂流瓶已经用完了，已经重新放出之前捡过的漂流瓶～\n" + prefix_message

            yield self.message_formatter.create_bottle_message(event, bottle, prefix_message)
        except Exception as e:
            logger.error(f"Failed to pick cloud bottle: {e}")
            yield event.plain_result("抱歉，捡云漂流瓶时遇到了问题，请稍后再试...")

    async def terminate(self):
        """插件终止时的清理工作"""
        self.stop_sync_task()
        self.stop_cron_notify()
        await self.image_handler.close()
        await self.cloud_storage.close()
