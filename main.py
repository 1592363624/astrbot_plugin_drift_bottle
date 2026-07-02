from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, register
from astrbot.api import logger, AstrBotConfig
from typing import Optional, List, Dict
import asyncio
import time
import random
from datetime import datetime, date

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
        
        # 如果启用了云同步，启动定时同步任务
        if self.config_manager.is_cloud_sync_enabled():
            self.start_sync_task()

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

    async def _send_group_notification(self, event: AstrMessageEvent):
        """发送漂流瓶群通知"""
        # 检查是否启用群通知
        if not self.config_manager.is_group_notify_enabled():
            return
        
        # 检查频率限制
        if not self._check_notify_frequency():
            return
        
        try:
            # 获取 bot 实例
            bot = event.bot
            
            # 获取群列表
            group_list_result = await bot.call_action("get_group_list")
            
            if not group_list_result or "data" not in group_list_result:
                logger.warning("获取群列表失败或返回数据为空")
                return
            
            groups = group_list_result.get("data", [])
            if not groups:
                logger.info("机器人没有加入任何群，无法发送通知")
                return
            
            # 随机选择一个群
            target_group = random.choice(groups)
            group_id = target_group.get("group_id")
            group_name = target_group.get("group_name", "未知群")
            
            if not group_id:
                logger.warning("选中的群没有有效的 group_id")
                return
            
            # 获取通知消息内容
            notify_message = self.config_manager.get_group_notify_message()
            
            # 发送群消息
            await bot.send_group_msg(
                group_id=group_id,
                message=[{"type": "text", "data": {"text": notify_message}}]
            )
            
            # 更新频率控制状态
            self._notify_last_time = time.time()
            self._notify_today_count += 1
            
            logger.info(
                f"已向群 {group_name}({group_id}) 发送漂流瓶通知"
                f"（今日第 {self._notify_today_count} 次）"
            )
            
        except Exception as e:
            logger.error(f"发送群通知失败: {str(e)}")

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
        await self.image_handler.close()
        await self.cloud_storage.close()
