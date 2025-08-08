import asyncio
import base64
import random
import aiohttp
import time
import re
from collections import OrderedDict
from meme_generator import (
    DeserializeError,
    ImageAssetMissing,
    ImageDecodeError,
    ImageEncodeError,
    ImageNumberMismatch,
    MemeFeedback,
    TextNumberMismatch,
    TextOverLength,
)
from meme_generator import Meme, get_memes
from meme_generator import Image as MemeImage
from meme_generator.resources import check_resources_in_background
from meme_generator.tools import MemeProperties, MemeSortBy, render_meme_list
from astrbot import logger
from astrbot.api.event import filter
from astrbot.api.star import Context, Star, register
from astrbot.core import AstrBotConfig
from astrbot.core.platform import AstrMessageEvent

import io
from typing import List, Union
import astrbot.core.message.components as Comp
from astrbot.core.star.filter.event_message_type import EventMessageType
from PIL import Image


@register(
    "astrbot_plugin_memelite_rs",
    "Zhalslar",
    "表情包生成器，制作各种沙雕表情（Rust重构版，速度快占用小），支持黑白名单管理和参数解析，支持自定义管理员列表，适配AstrBot ",
    "2.0.6",
    "https://github.com/Zhalslar/astrbot_plugin_memelite_rs",
)
class MemePlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config

        self.use_whitelist: bool = config.get("use_whitelist", False)
        self.memes_disabled_list: list[str] = config.get("memes_disabled_list", [])
        self.memes_enabled_list: list[str] = config.get("memes_enabled_list", [])
        self.require_admin_for_management: bool = config.get("require_admin_for_management", True)
        self.admin_users: list[str] = config.get("admin_users", [])
        self.sort_by_str: str = config.get("sort_by_str", "key")

        self.memes: list[Meme] = get_memes()
        self.meme_keywords = [
            keyword for meme in self.memes for keyword in meme.info.keywords
        ]

        self.prefix: str = config.get("prefix", "")

        self.fuzzy_match: int = config.get("fuzzy_match", True)
        self.is_compress_image: bool = config.get("is_compress_image", True)

        self.is_check_resources: bool = config.get("is_check_resources", True)
        if self.is_check_resources:
            logger.info("正在检查memes资源文件...")
            check_resources_in_background()

        # 头像缓存，使用 OrderedDict 实现 FIFO
        self._avatar_cache: OrderedDict[str, tuple[bytes, float]] = OrderedDict()
        self._max_cache_size: int = config.get("avatar_cache_max_count", 50)
        self._max_cache_size_bytes: int = config.get("avatar_cache_max_size_mb", 20) * 1024 * 1024
        
        logger.info(f"头像缓存已初始化，最大缓存数量: {self._max_cache_size}，最大内存占用: {self._max_cache_size_bytes // 1024 // 1024} MB")

    def _is_admin(self, event: AstrMessageEvent) -> bool:
        """检查用户是否为管理员"""
        # 如果配置中不要求管理员权限，则所有用户都可以使用
        if not self.require_admin_for_management:
            return True
        
        # 如果管理员列表为空，则所有用户都可以使用（向后兼容）
        if not self.admin_users:
            return True
            
        try:
            # 获取用户ID
            user_id = str(event.get_sender_id())
            # 检查用户ID是否在管理员列表中
            return user_id in self.admin_users
        except Exception as e:
            logger.warning(f"获取用户ID失败: {e}")
            # 如果无法获取用户ID信息，默认不是管理员
            return False

    def _parse_meme_options(self, meme: Meme, text_parts: list[str]) -> tuple[list[str], dict[str, Union[bool, str, int, float]]]:
        """动态解析meme选项参数 - 根据meme的实际参数定义进行解析"""
        options = {}
        remaining_texts = []
        
        # 尝试获取meme的参数定义
        meme_args = []
        try:
            if hasattr(meme.info, 'params') and hasattr(meme.info.params, 'args'):
                meme_args = meme.info.params.args
            elif hasattr(meme.info, 'params') and hasattr(meme.info.params, 'options'):
                meme_args = meme.info.params.options
            
            if not meme_args:
                # 如果没有参数定义，使用基本的通用解析
                return self._parse_basic_options(text_parts)
                
        except Exception as e:
            logger.debug(f"无法获取meme参数定义: {e}")
            return self._parse_basic_options(text_parts)
        
        # 构建参数映射表
        param_map = {}
        used_short_params = set()  # 跟踪已使用的短参数
        
        for arg in meme_args:
            # 主要参数名
            if hasattr(arg, 'name'):
                param_map[arg.name] = arg
                param_map[arg.name.replace('_', '-')] = arg  # 支持短横线格式
                
                # 自动生成短参数：取参数名的首字母作为短参数，避免冲突
                short_param = arg.name[0].lower()
                if short_param not in used_short_params:
                    param_map[short_param] = arg
                    used_short_params.add(short_param)
            
            # 别名支持 - 使用meme定义中的aliases
            if hasattr(arg, 'aliases') and arg.aliases:
                for alias in arg.aliases:
                    # 直接使用aliases中定义的别名
                    param_map[alias] = arg
        
        i = 0
        while i < len(text_parts):
            text = text_parts[i]
            consumed = False
            
            if text.startswith('-'):
                # 处理参数格式
                param_name = text.lstrip('-')
                
                # 查找匹配的参数定义
                matched_arg = param_map.get(param_name)
                if matched_arg:
                    try:
                        consumed, skip_next = self._parse_single_param(
                            matched_arg, param_name, text_parts, i, options
                        )
                        if skip_next:
                            i += 1  # 跳过值参数
                    except Exception as e:
                        logger.debug(f"解析参数 {param_name} 时出错: {e}")
                        # 解析失败时采用通用方法
                        consumed = self._parse_generic_param(param_name, text_parts, i, options)
                        if consumed and i + 1 < len(text_parts) and not text_parts[i + 1].startswith('-'):
                            i += 1
                else:
                    # 未找到参数定义，使用通用解析
                    consumed = self._parse_generic_param(param_name, text_parts, i, options)
                    if consumed and i + 1 < len(text_parts) and not text_parts[i + 1].startswith('-'):
                        i += 1
            
            if not consumed:
                remaining_texts.append(text)
            
            i += 1
        
        # 输出调试信息
        if options:
            logger.debug(f"解析到的meme选项: {options}")
        
        return remaining_texts, options
    
    def _parse_single_param(self, arg, param_name: str, text_parts: list[str], index: int, options: dict) -> tuple[bool, bool]:
        """解析单个参数，返回(是否消费, 是否跳过下一个)"""
        # 获取参数类型和默认值
        param_type = getattr(arg, 'type', None) if hasattr(arg, 'type') else None
        default_value = getattr(arg, 'default', None) if hasattr(arg, 'default') else None
        
        # 获取实际的参数名（用于存储到options中）
        actual_name = getattr(arg, 'name', param_name) if hasattr(arg, 'name') else param_name
        
        # 根据参数类型解析
        if param_type == 'bool' or (default_value is not None and isinstance(default_value, bool)):
            # 布尔类型参数
            options[actual_name] = True
            return True, False
        
        elif param_type in ['int', 'float'] or (default_value is not None and isinstance(default_value, (int, float))):
            # 数值类型参数
            if index + 1 < len(text_parts) and not text_parts[index + 1].startswith('-'):
                try:
                    value = text_parts[index + 1]
                    if param_type == 'int' or isinstance(default_value, int):
                        options[actual_name] = int(value)
                    else:
                        options[actual_name] = float(value)
                    return True, True
                except ValueError:
                    # 解析失败，当作布尔参数
                    options[actual_name] = True
                    return True, False
            else:
                # 没有值，当作布尔参数
                options[actual_name] = True
                return True, False
        
        elif param_type == 'str' or (default_value is not None and isinstance(default_value, str)):
            # 字符串类型参数
            if index + 1 < len(text_parts) and not text_parts[index + 1].startswith('-'):
                options[actual_name] = text_parts[index + 1]
                return True, True
            else:
                # 没有值，当作布尔参数
                options[actual_name] = True
                return True, False
        
        else:
            # 未知类型，尝试智能解析
            if index + 1 < len(text_parts) and not text_parts[index + 1].startswith('-'):
                value = text_parts[index + 1]
                # 尝试解析为合适的类型
                try:
                    if '.' in value:
                        options[actual_name] = float(value)
                    elif value.isdigit() or (value.startswith('-') and value[1:].isdigit()):
                        options[actual_name] = int(value)
                    elif value.lower() in ['true', 'false']:
                        options[actual_name] = value.lower() == 'true'
                    else:
                        options[actual_name] = value
                    return True, True
                except ValueError:
                    options[actual_name] = value
                    return True, True
            else:
                # 没有值，当作布尔参数
                options[actual_name] = True
                return True, False
    
    def _parse_generic_param(self, param_name: str, text_parts: list[str], index: int, options: dict) -> bool:
        """通用参数解析方法（当无法从meme定义获取参数信息时使用）"""
        # 不使用通用短参数，只支持基本的参数解析
        # 尝试获取值
        if index + 1 < len(text_parts) and not text_parts[index + 1].startswith('-'):
            value = text_parts[index + 1]
            try:
                if '.' in value:
                    options[param_name] = float(value)
                elif value.isdigit() or (value.startswith('-') and value[1:].isdigit()):
                    options[param_name] = int(value)
                elif value.lower() in ['true', 'false']:
                    options[param_name] = value.lower() == 'true'
                else:
                    options[param_name] = value
            except ValueError:
                options[param_name] = value
            return True
        else:
            # 没有值，当作布尔参数
            options[param_name] = True
            return True
    
    def _parse_basic_options(self, text_parts: list[str]) -> tuple[list[str], dict[str, Union[bool, str, int, float]]]:
        """基础的选项解析（当meme没有参数定义时使用）"""
        options = {}
        remaining_texts = []
        
        i = 0
        while i < len(text_parts):
            text = text_parts[i]
            if text.startswith('-'):
                param_name = text.lstrip('-')
                consumed = self._parse_generic_param(param_name, text_parts, i, options)
                if consumed and i + 1 < len(text_parts) and not text_parts[i + 1].startswith('-'):
                    # 检查是否消费了下一个参数作为值
                    next_val = text_parts[i + 1]
                    if param_name in options and options[param_name] == next_val:
                        i += 1  # 跳过已消费的值
            else:
                remaining_texts.append(text)
            i += 1
        
        return remaining_texts, options

    def _is_meme_available(self, keyword: str) -> bool:
        """判断meme是否可用"""
        if self.use_whitelist:
            # 白名单模式：只有在白名单中的才可用
            return keyword in self.memes_enabled_list
        else:
            # 黑名单模式：不在黑名单中的都可用
            return keyword not in self.memes_disabled_list

    def _process_meme_operation(self, meme_names: tuple[str], operation: str) -> tuple[list[str], list[str], list[str]]:
        """处理meme启用/禁用操作的通用逻辑
        
        Args:
            meme_names: meme名称元组
            operation: 操作类型 ('enable' 或 'disable')
            
        Returns:
            tuple: (成功处理的meme列表, 无效的meme列表, 已处于目标状态的meme列表)
        """
        valid_memes = []
        invalid_memes = []
        already_in_state = []
        
        for meme_name in meme_names:
            if meme_name not in self.meme_keywords:
                invalid_memes.append(meme_name)
                continue
            
            if operation == 'enable':
                if self.use_whitelist:
                    # 白名单模式：添加到白名单
                    if meme_name in self.memes_enabled_list:
                        already_in_state.append(meme_name)
                    else:
                        self.memes_enabled_list.append(meme_name)
                        valid_memes.append(meme_name)
                else:
                    # 黑名单模式：从黑名单中移除
                    if meme_name not in self.memes_disabled_list:
                        already_in_state.append(meme_name)
                    else:
                        self.memes_disabled_list.remove(meme_name)
                        valid_memes.append(meme_name)
            
            elif operation == 'disable':
                if self.use_whitelist:
                    # 白名单模式：从白名单中移除
                    if meme_name not in self.memes_enabled_list:
                        already_in_state.append(meme_name)
                    else:
                        self.memes_enabled_list.remove(meme_name)
                        valid_memes.append(meme_name)
                else:
                    # 黑名单模式：添加到黑名单
                    if meme_name in self.memes_disabled_list:
                        already_in_state.append(meme_name)
                    else:
                        self.memes_disabled_list.append(meme_name)
                        valid_memes.append(meme_name)
        
        # 如果有变更，保存配置
        if valid_memes:
            self.config.save_config(replace_config=self.config)
        
        return valid_memes, invalid_memes, already_in_state

    def _format_operation_result(self, operation: str, meme_names: tuple[str], 
                               valid_memes: list[str], invalid_memes: list[str], 
                               already_in_state: list[str]) -> str:
        """格式化操作结果消息
        
        Args:
            operation: 操作类型 ('enable' 或 'disable')
            meme_names: 原始输入的meme名称
            valid_memes: 成功处理的meme
            invalid_memes: 无效的meme
            already_in_state: 已处于目标状态的meme
            
        Returns:
            str: 格式化后的结果消息
        """
        op_text = "启用" if operation == 'enable' else "禁用"
        state_text = "已启用" if operation == 'enable' else "已禁用"
        
        if len(meme_names) == 1:
            # 单个操作的简洁消息
            if valid_memes:
                return f"已{op_text}meme: {valid_memes[0]}"
            elif already_in_state:
                return f"meme: {already_in_state[0]} {state_text}"
            elif invalid_memes:
                return f"meme: {invalid_memes[0]} 不存在"
        else:
            # 批量操作的详细消息
            result_msg = f"批量{op_text}meme结果：\n"
            if valid_memes:
                result_msg += f"✅ 成功{op_text}：{', '.join(valid_memes)}\n"
            if already_in_state:
                result_msg += f"⚠️ {state_text}：{', '.join(already_in_state)}\n"
            if invalid_memes:
                result_msg += f"❌ 无效的meme：{', '.join(invalid_memes)}"
            return result_msg

    def _get_target_user_id(self, event: AstrMessageEvent, user_id: str = None) -> tuple[str, str]:
        """获取目标用户ID（从@用户或参数中）
        
        Returns:
            tuple: (target_user_id, error_message) - 如果error_message不为空则表示出错
        """
        target_user_id = None
        
        # 优先从消息中获取@的用户ID
        messages = event.get_messages()
        at_seg = next((seg for seg in messages if isinstance(seg, Comp.At)), None)
        if at_seg:
            target_user_id = str(at_seg.qq)
        elif user_id:
            # 如果没有@用户，使用传入的用户ID参数
            target_user_id = user_id
        
        if not target_user_id:
            return None, "请@要操作的用户或提供用户ID"
        
        return target_user_id, ""

    def _parse_import_text(self, list_text: str) -> list[str]:
        """解析导入文本，支持多种分隔符
        
        Args:
            list_text: 待解析的文本
            
        Returns:
            list[str]: 解析后的名称列表
        """
        import_names = []
        for separator in [',', '，', ' ']:
            if separator in list_text:
                import_names = [name.strip() for name in list_text.split(separator) if name.strip()]
                break
        
        if not import_names:
            import_names = [list_text.strip()]
        
        return import_names

    def _get_current_list_info(self) -> tuple[list[str], str]:
        """获取当前名单信息
        
        Returns:
            tuple: (当前名单, 模式名称)
        """
        if self.use_whitelist:
            return self.memes_enabled_list, "白名单"
        else:
            return self.memes_disabled_list, "黑名单"

    @filter.command("meme帮助", alias={"表情帮助"})
    async def memes_help(self, event: AstrMessageEvent):
        "查看有哪些关键词可以触发meme"
        sort_by_map = {
        "key": MemeSortBy.Key,
        "keywords": MemeSortBy.Keywords,
        "keywords_pinyin": MemeSortBy.KeywordsPinyin,
        "date_created": MemeSortBy.DateCreated,
        "date_modified": MemeSortBy.DateModified
        }
        sort_by = sort_by_map.get(self.sort_by_str) or MemeSortBy.KeywordsPinyin

        # 过滤出可用的meme
        available_memes = []
        exclude_memes = []
        
        for meme in self.memes:
            # 检查meme的任意一个关键词是否可用
            meme_available = any(self._is_meme_available(keyword) for keyword in meme.info.keywords)
            if meme_available:
                available_memes.append(meme)
            else:
                exclude_memes.append(meme.key)

        meme_properties: dict[str, MemeProperties] = {}
        for meme in available_memes:
            properties = MemeProperties(disabled=False, hot=False, new=False)
            meme_properties[meme.key] = properties

        # 使用 asyncio.to_thread 来运行同步函数
        output: bytes | None = await asyncio.to_thread(
            render_meme_list,  # type: ignore
            meme_properties=meme_properties,
            exclude_memes=exclude_memes,
            sort_by=sort_by,
            sort_reverse=False,
            text_template="{index}. {keywords}",
            add_category_icon=True,
        )
        if output:
            mode = "白名单" if self.use_whitelist else "黑名单"
            total_count = len(self.memes)
            available_count = len(available_memes)
            yield event.chain_result([
                Comp.Plain(f"当前模式：{mode} | 可用meme：{available_count}/{total_count}\n"),
                Comp.Image.fromBytes(output)
            ])
        else:
            yield event.plain_result("meme列表图生成失败")


    @filter.command("meme详情", alias={"表情详情"})
    async def meme_details_show(
        self, event: AstrMessageEvent, keyword: str | int | None = None
    ):
        "查看指定meme需要的参数"
        if not keyword:
            yield event.plain_result("未指定要查看的meme")
            return
        keyword = str(keyword)
        target_keyword = next((k for k in self.meme_keywords if k == keyword), None)
        if target_keyword is None:
            yield event.plain_result("未支持的meme关键词")
            return
        
        # 检查meme是否可用
        if not self._is_meme_available(target_keyword):
            mode = "白名单" if self.use_whitelist else "黑名单"
            yield event.plain_result(f"该meme在当前{mode}模式下不可用")
            return

        # 匹配meme
        meme = self._find_meme(keyword)
        if not meme:
            yield event.plain_result("未找到相关meme")
            return

        # 提取meme的所有参数
        name = meme.key
        info = meme.info
        params = info.params
        keywords = info.keywords
        min_images = params.min_images
        max_images = params.max_images
        min_texts = params.min_texts
        max_texts = params.max_texts
        default_texts = params.default_texts
        tags = info.tags

        meme_info = ""
        if name:
            meme_info += f"名称：{name}\n"

        if keywords:
            meme_info += f"别名：{keywords}\n"

        if max_images > 0:
            meme_info += (
                f"所需图片：{min_images}张\n"
                if min_images == max_images
                else f"所需图片：{min_images}~{max_images}张\n"
            )

        if max_texts > 0:
            meme_info += (
                f"所需文本：{min_texts}段\n"
                if min_texts == max_texts
                else f"所需文本：{min_texts}~{max_texts}段\n"
            )

        if default_texts:
            meme_info += f"默认文本：{default_texts}\n"

        if tags:
            meme_info += f"标签：{list(tags)}\n"

        # 添加参数选项信息
        meme_args = []
        try:
            if hasattr(info.params, 'args') and info.params.args:
                meme_args = info.params.args
            elif hasattr(info.params, 'options') and info.params.options:
                meme_args = info.params.options
        except:
            pass
        
        if meme_args:
            meme_info += f"\n可用参数 ({len(meme_args)}个)：\n"
            
            # 先构建短参数映射以检查冲突
            used_short_params = set()
            short_param_map = {}
            for arg in meme_args:
                if hasattr(arg, 'name'):
                    short_param = arg.name[0].lower()
                    if short_param not in used_short_params:
                        short_param_map[arg.name] = short_param
                        used_short_params.add(short_param)
            
            for i, arg in enumerate(meme_args):
                arg_line = f"• "
                
                # 参数名和别名
                if hasattr(arg, 'name'):
                    # 显示自动生成的短参数（如果没有冲突）
                    if arg.name in short_param_map:
                        arg_line += f"-{short_param_map[arg.name]}/"
                    
                    # 添加aliases中的短参数
                    if hasattr(arg, 'aliases') and arg.aliases:
                        short_aliases = []
                        for alias in arg.aliases:
                            clean_alias = alias.lstrip('-')
                            if len(clean_alias) <= 2:  # 短参数
                                # 避免与自动生成的短参数重复
                                if arg.name not in short_param_map or clean_alias != short_param_map[arg.name]:
                                    short_aliases.append(clean_alias)
                        if short_aliases:
                            arg_line += f"{'/'.join(short_aliases)}/"
                    
                    arg_line += f"--{arg.name}"
                
                # 参数详细信息
                param_details = []
                
                # 默认值
                if hasattr(arg, 'default') and arg.default is not None:
                    param_details.append(f"默认:{arg.default}")
                
                # 最小值和最大值
                if hasattr(arg, 'minimum') and arg.minimum is not None:
                    param_details.append(f"最小值:{arg.minimum}")
                if hasattr(arg, 'maximum') and arg.maximum is not None:
                    param_details.append(f"最大值:{arg.maximum}")
                
                # 可选值
                if hasattr(arg, 'choices') and arg.choices:
                    choices_str = "/".join(str(choice) for choice in arg.choices)
                    param_details.append(f"可选:{choices_str}")
                elif hasattr(arg, 'options') and arg.options:
                    choices_str = "/".join(str(option) for option in arg.options)
                    param_details.append(f"可选:{choices_str}")
                
                if param_details:
                    arg_line += f" [{';'.join(param_details)}]"
                
                meme_info += arg_line + "\n"
                
                # 限制显示数量避免信息过长
                if i >= 4:  # 只显示前5个参数
                    remaining = len(meme_args) - 5
                    if remaining > 0:
                        meme_info += f"  ... 还有 {remaining} 个参数\n"
                    break

        preview: bytes = meme.generate_preview()  # type: ignore
        chain = [
            Comp.Plain(meme_info),
            Comp.Image.fromBytes(preview),
        ]
        yield event.chain_result(chain)

    @filter.command("禁用meme", alias={"添加到黑名单"})
    async def add_to_blacklist(
        self, event: AstrMessageEvent, *meme_names
    ):
        """禁用meme（支持单个或批量操作）"""
        if not self._is_admin(event):
            yield event.plain_result("❌ 此命令需要管理员权限")
            return
            
        if not meme_names:
            yield event.plain_result("请指定要禁用的meme名称\n单个：禁用meme 摸鱼\n批量：禁用meme 摸鱼 鸽子 加班")
            return
        
        # 使用通用处理函数
        valid_memes, invalid_memes, already_disabled = self._process_meme_operation(meme_names, 'disable')
        
        # 使用通用格式化函数
        result_msg = self._format_operation_result('disable', meme_names, valid_memes, invalid_memes, already_disabled)
        yield event.plain_result(result_msg)
        
        logger.info(f"禁用meme: {valid_memes}")

    @filter.command("启用meme", alias={"添加到白名单"})
    async def add_to_whitelist(
        self, event: AstrMessageEvent, *meme_names
    ):
        """启用meme（支持单个或批量操作）"""
        if not self._is_admin(event):
            yield event.plain_result("❌ 此命令需要管理员权限")
            return
            
        if not meme_names:
            yield event.plain_result("请指定要启用的meme名称\n单个：启用meme 摸鱼\n批量：启用meme 摸鱼 鸽子 加班")
            return
        
        # 使用通用处理函数
        valid_memes, invalid_memes, already_enabled = self._process_meme_operation(meme_names, 'enable')
        
        # 使用通用格式化函数
        result_msg = self._format_operation_result('enable', meme_names, valid_memes, invalid_memes, already_enabled)
        yield event.plain_result(result_msg)
        
        logger.info(f"启用meme: {valid_memes}")

    @filter.command("meme名单", alias={"meme黑名单", "meme白名单"})
    async def list_meme_list(self, event: AstrMessageEvent):
        """查看当前的meme名单"""
        current_list, mode = self._get_current_list_info()
        yield event.plain_result(f"当前模式：{mode}\n{mode}meme: {current_list if current_list else '空'}")

    @filter.command("切换名单模式")
    async def toggle_list_mode(self, event: AstrMessageEvent):
        """切换黑白名单模式"""
        if not self._is_admin(event):
            yield event.plain_result("❌ 此命令需要管理员权限")
            return
            
        self.use_whitelist = not self.use_whitelist
        self.config.set("use_whitelist", self.use_whitelist)
        self.config.save_config(replace_config=self.config)
        
        mode = "白名单" if self.use_whitelist else "黑名单"
        yield event.plain_result(f"已切换到 {mode} 模式")
        logger.info(f"meme名单模式已切换到: {mode}")

    @filter.command("清空名单")
    async def clear_meme_list(self, event: AstrMessageEvent):
        """清空当前模式下的名单"""
        if not self._is_admin(event):
            yield event.plain_result("❌ 此命令需要管理员权限")
            return
        
        current_list, mode = self._get_current_list_info()
        count = len(current_list)
        current_list.clear()
        self.config.save_config(replace_config=self.config)
        yield event.plain_result(f"已清空{mode}，共清理了 {count} 个meme")
        logger.info(f"{mode}已清空")



    @filter.command("按标签管理名单")
    async def manage_list_by_tag(self, event: AstrMessageEvent, action: str = None, tag: str = None):
        """按标签批量管理名单（添加/移除）"""
        if not self._is_admin(event):
            yield event.plain_result("❌ 此命令需要管理员权限")
            return
            
        if not action or not tag or action not in ["添加", "移除", "启用", "禁用"]:
            yield event.plain_result("用法：按标签管理名单 [添加/移除/启用/禁用] [标签名]\n例如：按标签管理名单 启用 动物")
            return
        
        # 找到包含指定标签的所有meme
        tagged_memes = []
        for meme in self.memes:
            if tag in meme.info.tags:
                # 获取第一个关键词作为代表
                if meme.info.keywords:
                    tagged_memes.append(meme.info.keywords[0])
        
        if not tagged_memes:
            yield event.plain_result(f"没有找到标签为 '{tag}' 的meme")
            return
        
        # 将操作转换为标准格式
        operation = None
        if action in ["添加", "启用"]:
            operation = 'enable'
        elif action in ["移除", "禁用"]:
            operation = 'disable'
        
        # 使用通用处理函数
        valid_memes, invalid_memes, already_in_state = self._process_meme_operation(tuple(tagged_memes), operation)
        
        # 生成结果消息
        op_text = "启用" if operation == 'enable' else "禁用"
        if valid_memes:
            yield event.plain_result(f"按标签 '{tag}' {op_text}成功：\n{', '.join(valid_memes)}\n共 {len(valid_memes)} 个meme")
        else:
            yield event.plain_result(f"标签 '{tag}' 下的所有meme都已处于目标状态")
        
        logger.info(f"按标签 {tag} {op_text}: {valid_memes}")

    @filter.command("导入名单")
    async def import_meme_list(self, event: AstrMessageEvent, list_text: str = None):
        """从文本导入meme名单（用逗号或空格分隔）"""
        if not self._is_admin(event):
            yield event.plain_result("❌ 此命令需要管理员权限")
            return
            
        if not list_text:
            yield event.plain_result("请提供要导入的meme名单，用逗号或空格分隔\n例如：导入名单 摸鱼,鸽子,加班")
            return
        
        # 使用通用解析函数
        import_names = self._parse_import_text(list_text)
        current_list, mode = self._get_current_list_info()
        
        valid_memes = []
        invalid_memes = []
        already_exists = []
        
        for meme_name in import_names:
            if meme_name not in self.meme_keywords:
                invalid_memes.append(meme_name)
                continue
            
            if meme_name in current_list:
                already_exists.append(meme_name)
            else:
                current_list.append(meme_name)
                valid_memes.append(meme_name)
        
        if valid_memes:
            self.config.save_config(replace_config=self.config)
        
        result_msg = f"导入{mode}结果：\n"
        if valid_memes:
            result_msg += f"✅ 成功导入：{len(valid_memes)} 个meme\n"
        if already_exists:
            result_msg += f"⚠️ 已存在：{len(already_exists)} 个meme\n"
        if invalid_memes:
            result_msg += f"❌ 无效meme：{len(invalid_memes)} 个\n"
        
        result_msg += f"\n当前{mode}共有：{len(current_list)} 个meme"
        
        yield event.plain_result(result_msg)
        logger.info(f"导入{mode}: {valid_memes}")

    @filter.command("导出名单")
    async def export_meme_list(self, event: AstrMessageEvent):
        """导出当前名单为文本格式"""
        current_list, mode = self._get_current_list_info()
        
        if not current_list:
            yield event.plain_result(f"当前{mode}为空")
            return
        
        # 生成导出文本
        export_text = ", ".join(current_list)
        result_msg = f"当前{mode}导出（共{len(current_list)}个）：\n\n{export_text}\n\n复制上述内容可用于导入名单命令"
        
        yield event.plain_result(result_msg)

    @filter.command("meme权限状态")
    async def show_permission_status(self, event: AstrMessageEvent):
        """显示当前权限配置和用户权限状态"""
        # 显示配置状态
        admin_required = "是" if self.require_admin_for_management else "否"
        status_msg = f"管理命令权限设置：{admin_required}\n"
        
        # 显示管理员列表配置
        if self.require_admin_for_management:
            if self.admin_users:
                admin_count = len(self.admin_users)
                status_msg += f"管理员列表：{admin_count} 个用户\n"
                # 显示前几个管理员ID（保护隐私）
                if admin_count <= 3:
                    status_msg += f"管理员ID：{', '.join(self.admin_users)}\n"
                else:
                    status_msg += f"管理员ID：{', '.join(self.admin_users[:3])} 等{admin_count}个\n"
            else:
                status_msg += "管理员列表：空（所有用户都可使用管理命令）\n"
        
        # 显示当前用户状态
        try:
            user_id = str(event.get_sender_id())
            status_msg += f"您的用户ID：{user_id}\n"
            
            # 判断是否可以使用管理命令
            can_use_admin = self._is_admin(event)
            admin_access = "是" if can_use_admin else "否"
            status_msg += f"可使用管理命令：{admin_access}"
            
        except Exception as e:
            status_msg += f"无法获取用户ID信息：{e}"
        
        yield event.plain_result(status_msg)


    

    


    @filter.command("添加管理员")
    async def add_admin(self, event: AstrMessageEvent, user_id: str = None):
        """添加管理员用户"""
        if not self._is_admin(event):
            yield event.plain_result("❌ 此命令需要管理员权限")
            return
        
        # 使用通用用户ID获取函数
        target_user_id, error_msg = self._get_target_user_id(event, user_id)
        if error_msg:
            yield event.plain_result(f"{error_msg}，例如：添加管理员 @用户 或 添加管理员 123456789")
            return
        
        if target_user_id in self.admin_users:
            yield event.plain_result(f"用户 {target_user_id} 已经是管理员")
            return
        
        self.admin_users.append(target_user_id)
        self.config.save_config(replace_config=self.config)
        yield event.plain_result(f"✅ 已添加管理员：{target_user_id}")
        logger.info(f"添加管理员：{target_user_id}，当前管理员列表：{self.admin_users}")

    @filter.command("移除管理员")
    async def remove_admin(self, event: AstrMessageEvent, user_id: str = None):
        """移除管理员用户"""
        if not self._is_admin(event):
            yield event.plain_result("❌ 此命令需要管理员权限")
            return
        
        # 使用通用用户ID获取函数
        target_user_id, error_msg = self._get_target_user_id(event, user_id)
        if error_msg:
            yield event.plain_result(f"{error_msg}，例如：移除管理员 @用户 或 移除管理员 123456789")
            return
        
        if target_user_id not in self.admin_users:
            yield event.plain_result(f"用户 {target_user_id} 不是管理员")
            return
        
        self.admin_users.remove(target_user_id)
        self.config.save_config(replace_config=self.config)
        yield event.plain_result(f"✅ 已移除管理员：{target_user_id}")
        logger.info(f"移除管理员：{target_user_id}，当前管理员列表：{self.admin_users}")

    @filter.command("管理员列表")
    async def list_admins(self, event: AstrMessageEvent):
        """查看管理员列表"""
        if not self.admin_users:
            yield event.plain_result("当前没有设置管理员，所有用户都可以使用管理命令")
            return
        
        admin_list = "\n".join([f"• {admin_id}" for admin_id in self.admin_users])
        yield event.plain_result(f"当前管理员列表（共{len(self.admin_users)}个）：\n{admin_list}")

    @filter.command("清空管理员")
    async def clear_admins(self, event: AstrMessageEvent):
        """清空管理员列表"""
        if not self._is_admin(event):
            yield event.plain_result("❌ 此命令需要管理员权限")
            return
        
        if not self.admin_users:
            yield event.plain_result("管理员列表已经为空")
            return
        
        admin_count = len(self.admin_users)
        self.admin_users.clear()
        self.config.save_config(replace_config=self.config)
        yield event.plain_result(f"✅ 已清空管理员列表，共清理了 {admin_count} 个管理员\n注意：现在所有用户都可以使用管理命令")
        logger.info(f"管理员列表已清空，共清理了 {admin_count} 个管理员")


    @filter.command("清空头像缓存", alias={"清理头像缓存"})
    async def clear_avatar_cache(self, event: AstrMessageEvent):
        """清空头像缓存"""
        cache_count = len(self._avatar_cache)
        self._avatar_cache.clear()
        yield event.plain_result(f"已清空头像缓存，共清理了 {cache_count} 个头像")
        logger.info(f"头像缓存已清空，共清理了 {cache_count} 个头像")

    @filter.command("删除头像缓存", alias={"移除头像缓存"})
    async def remove_avatar_cache(self, event: AstrMessageEvent, user_id: str | None = None):
        """删除指定用户的头像缓存"""
        # 使用通用用户ID获取函数
        target_user_id, error_msg = self._get_target_user_id(event, user_id)
        if error_msg:
            yield event.plain_result(f"{error_msg}，例如：删除头像缓存 @用户 或 删除头像缓存 123456789")
            return
        
        if target_user_id in self._avatar_cache:
            del self._avatar_cache[target_user_id]
            yield event.plain_result(f"已删除用户 {target_user_id} 的头像缓存")
            logger.info(f"已删除用户 {target_user_id} 的头像缓存，当前缓存数量: {len(self._avatar_cache)}")
        else:
            yield event.plain_result(f"用户 {target_user_id} 的头像缓存不存在")

    @filter.command("查看头像缓存", alias={"头像缓存状态"})
    async def show_avatar_cache_status(self, event: AstrMessageEvent):
        """查看头像缓存状态"""
        cache_count = len(self._avatar_cache)
        max_count = self._max_cache_size
        max_size_mb = self._max_cache_size_bytes // 1024 // 1024
        
        if self._max_cache_size <= 0:
            yield event.plain_result("头像缓存已禁用")
            return
        
        if cache_count == 0:
            yield event.plain_result(f"头像缓存为空\n最大数量限制: {max_count}\n最大内存限制: {max_size_mb} MB")
        else:
            # 计算缓存中每个头像的大小
            total_size = self._get_cache_size_bytes()
            size_mb = total_size / 1024 / 1024
            avg_size_kb = total_size / cache_count / 1024
            
            cache_info = f"头像缓存状态:\n"
            cache_info += f"缓存数量: {cache_count}/{max_count}\n"
            cache_info += f"内存占用: {size_mb:.2f}/{max_size_mb} MB\n"
            cache_info += f"平均大小: {avg_size_kb:.1f} KB/个\n"
            cache_info += f"使用率: 数量 {cache_count/max_count*100:.1f}%，内存 {size_mb/max_size_mb*100:.1f}%"
            
            yield event.plain_result(cache_info)

    @filter.event_message_type(EventMessageType.ALL)
    async def meme_handle(self, event: AstrMessageEvent):
        """
        处理 meme 生成的主流程。

        功能描述：
        - 支持匹配所有 meme 关键词。
        - 支持从原始消息中提取参数, 空格隔开参数。
        - 支持引用消息传参 。
        - 自动获取消息发送者、被 @ 的用户以及 bot 自身的相关参数。
        """

        # 前缀模式
        if self.prefix:
            chain = event.get_messages()
            if not chain:
                return
            first_seg = chain[0]
            # 前缀触发
            if isinstance(first_seg, Comp.Plain):
                if not first_seg.text.startswith(self.prefix):
                    return
            if isinstance(first_seg, Comp.Reply) and len(chain) > 1:
                second_seg = chain[1]
                if isinstance(
                    second_seg, Comp.Plain
                ) and not second_seg.text.startswith(self.prefix):
                    return
            # @bot触发
            elif isinstance(first_seg, Comp.At):
                if str(first_seg.qq) != str(event.get_self_id()):
                    return
            else:
                return

        message_str = event.get_message_str().removeprefix(self.prefix)
        if not message_str:
            return

        if self.fuzzy_match:
            # 模糊匹配：检查关键词是否在消息字符串中
            keyword = next((k for k in self.meme_keywords if k in message_str), None)
        else:
            # 精确匹配：检查关键词是否等于消息字符串的第一个单词
            keyword = next(
                (k for k in self.meme_keywords if k == message_str.split()[0]), None
            )

        if not keyword or not self._is_meme_available(keyword):
            return

        # 匹配meme
        meme = self._find_meme(keyword)
        if not meme:
            yield event.plain_result("未找到相关meme")
            return

        # 收集参数
        meme_images, texts, options = await self._get_parms(event, keyword, meme)

        # 合成表情
        image: bytes = await self._meme_generate(meme, meme_images, texts, options)

        # 压缩图片
        if self.is_compress_image:
            try:
                image = self.compress_image(image) or image
            except:  # noqa: E722
                pass

        # 发送图片
        chain = [Comp.Image.fromBytes(image)]
        yield event.chain_result(chain)  # type: ignore

    def _find_meme(self, keyword: str) -> Meme | None:
        """根据关键词寻找meme"""
        for meme in self.memes:
            if keyword == meme.key or any(k == keyword for k in meme.info.keywords):
                return meme

    async def _get_parms(self, event: AstrMessageEvent, keyword: str, meme: Meme):
        """收集参数"""
        meme_images: list[MemeImage] = []
        texts: List[str] = []
        options: dict[str, Union[bool, str, int, float]] = {}

        params = meme.info.params
        min_images: int = params.min_images  # noqa: F841
        max_images: int = params.max_images
        min_texts: int = params.min_texts
        max_texts: int = params.max_texts
        default_texts: list[str] = params.default_texts

        messages = event.get_messages()
        send_id: str = event.get_sender_id()
        self_id: str = event.get_self_id()
        sender_name: str = str(event.get_sender_name())

        target_ids: list[str] = []
        target_names: list[str] = []
        all_text_parts: list[str] = []  # 收集所有文本用于参数解析

        async def _process_segment(_seg, name):
            """从消息段中获取参数"""
            if isinstance(_seg, Comp.Image):
                if hasattr(_seg, "url") and _seg.url:
                    img_url = _seg.url
                    if file_content := await self.download_image(img_url):
                        meme_images.append(MemeImage(name, file_content))

                elif hasattr(_seg, "file"):
                    file_content = _seg.file
                    if isinstance(file_content, str):
                        if file_content.startswith("base64://"):
                            file_content = file_content[len("base64://") :]
                        file_content = base64.b64decode(file_content)
                    if isinstance(file_content, bytes):
                        meme_images.append(MemeImage(name, file_content))

            elif isinstance(_seg, Comp.At):
                seg_qq = str(_seg.qq)
                if seg_qq != self_id:
                    target_ids.append(seg_qq)
                    if at_avatar := await self.get_avatar(event, seg_qq):
                        # 从消息平台获取At者的额外参数
                        if result := await self._get_extra(event, target_id=seg_qq):
                            nickname, sex = result
                            options["name"], options["gender"] = nickname, sex
                            target_names.append(nickname)
                            meme_images.append(MemeImage(nickname, at_avatar))

            elif isinstance(_seg, Comp.Plain):
                plains: list[str] = _seg.text.strip().split()
                for text in plains:
                    if text not in self.prefix and text != self.prefix + keyword:
                        all_text_parts.append(text)  # 收集到统一列表中

        # 如果有引用消息，也遍历之
        reply_seg = next((seg for seg in messages if isinstance(seg, Comp.Reply)), None)
        if reply_seg and reply_seg.chain:
            for seg in reply_seg.chain:
                await _process_segment(seg, "这家伙")

        # 遍历原始消息段落
        for seg in messages:
            await _process_segment(seg, sender_name)

        # 解析命令行参数并获取剩余文本
        remaining_texts, parsed_options = self._parse_meme_options(meme, all_text_parts)
        texts.extend(remaining_texts)
        options.update(parsed_options)

        # 从消息平台获取发送者的额外参数
        if not target_ids:
            if result := await self._get_extra(event, target_id=send_id):
                nickname, sex = result
                options["name"], options["gender"] = nickname, sex
                target_names.append(nickname)

        if not target_names:
            target_names.append(sender_name)

        # 确保图片数量在min_images到max_images之间(尽可能地获取图片)
        if len(meme_images) < max_images:
            if use_avatar := await self.get_avatar(event, send_id):
                meme_images.insert(0, MemeImage(sender_name, use_avatar))
        if len(meme_images) < max_images:
            if bot_avatar := await self.get_avatar(event, self_id):
                meme_images.insert(0, MemeImage("我", bot_avatar))
        meme_images = meme_images[:max_images]

        # 确保文本数量在min_texts到max_texts之间(文本参数足够即可)
        if len(texts) < min_texts and target_names:
            texts.extend(target_names)
        if len(texts) < min_texts and default_texts:
            texts.extend(default_texts)
        texts = texts[:max_texts]

        return meme_images, texts, options

    @staticmethod
    async def _meme_generate(
        meme: Meme, meme_images: list[MemeImage], texts: list[str], options
    ) -> bytes:
        """向meme生成器发出请求，返回生成的图片"""

        # 将同步函数运行在默认的线程池中
        result = await asyncio.to_thread(meme.generate, meme_images, texts, options)

        if result is None:
            logger.error("返回内容为空")
        elif isinstance(result, ImageDecodeError):
            logger.error(f"图片解码出错：{result.error}")
        elif isinstance(result, ImageEncodeError):
            logger.error(f"图片编码出错：{result.error}")
        elif isinstance(result, ImageAssetMissing):
            logger.error(f"缺少图片资源：{result.path}")
        elif isinstance(result, DeserializeError):
            logger.error(f"表情选项解析出错：{result.error}")
        elif isinstance(result, ImageNumberMismatch):
            num = (
                f"{result.min} ~ {result.max}"
                if result.min != result.max
                else str(result.min)
            )
            logger.error(f"图片数量不符，应为 {num}，实际传入 {result.actual}")
        elif isinstance(result, TextNumberMismatch):
            num = (
                f"{result.min} ~ {result.max}"
                if result.min != result.max
                else str(result.min)
            )
            logger.error(f"文字数量不符，应为 {num}，实际传入 {result.actual}")
        elif isinstance(result, TextOverLength):
            text = result.text
            repr = text if len(text) <= 10 else (text[:10] + "...")
            logger.error(f"文字过长：{repr}")
        elif isinstance(result, MemeFeedback):
            logger.error(result.feedback)

        if not isinstance(result, bytes):
            raise NotImplementedError

        return result

    def _get_cached_avatar(self, user_id: str) -> bytes | None:
        """从缓存中获取头像"""
        if user_id in self._avatar_cache:
            avatar_data, timestamp = self._avatar_cache[user_id]
            # 将访问的项移到末尾（更新访问时间）
            self._avatar_cache.move_to_end(user_id)
            return avatar_data
        return None

    def _get_cache_size_bytes(self) -> int:
        """获取当前缓存占用的总字节数"""
        return sum(len(avatar_data) for avatar_data, _ in self._avatar_cache.values())

    def _cache_avatar(self, user_id: str, avatar_data: bytes) -> None:
        """缓存头像数据"""
        # 如果缓存被禁用（max_cache_size为0），直接返回
        if self._max_cache_size <= 0:
            return
        
        # 如果已存在，先删除旧的
        if user_id in self._avatar_cache:
            del self._avatar_cache[user_id]
        
        # 检查数量限制：如果缓存已满，删除最旧的（第一个）
        while len(self._avatar_cache) >= self._max_cache_size:
            oldest_key = next(iter(self._avatar_cache))
            del self._avatar_cache[oldest_key]
            logger.debug(f"缓存数量已满，删除最旧的头像缓存: {oldest_key}")
        
        # 检查内存限制：如果添加新头像后会超过内存限制，删除最旧的缓存
        while self._avatar_cache and (self._get_cache_size_bytes() + len(avatar_data)) > self._max_cache_size_bytes:
            oldest_key = next(iter(self._avatar_cache))
            del self._avatar_cache[oldest_key]
            logger.debug(f"缓存内存已满，删除最旧的头像缓存: {oldest_key}")
        
        # 添加新的头像到缓存
        self._avatar_cache[user_id] = (avatar_data, time.time())
        logger.debug(f"头像已缓存: {user_id}，当前缓存数量: {len(self._avatar_cache)}，占用内存: {self._get_cache_size_bytes() // 1024} KB")

    @staticmethod
    async def _get_extra(event: AstrMessageEvent, target_id: str):
        """从消息平台获取参数"""
        if event.get_platform_name() == "aiocqhttp":
            from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import (
                AiocqhttpMessageEvent,
            )

            assert isinstance(event, AiocqhttpMessageEvent)
            client = event.bot
            user_info = await client.get_stranger_info(user_id=int(target_id))
            raw_nickname = user_info.get("nickname")
            nickname = str(raw_nickname if raw_nickname is not None else "Unknown")
            sex = user_info.get("sex")
            return nickname, sex
        # TODO 适配更多消息平台

    @staticmethod
    def compress_image(image: bytes, max_size: int = 512) -> bytes | None:
        """压缩静态图片或GIF到max_size大小"""
        try:
            # 将输入的bytes加载为图片
            img = Image.open(io.BytesIO(image))
            output = io.BytesIO()

            if img.format == "GIF":
                return
            else:
                # 如果是静态图片，检查尺寸并压缩
                if img.width > max_size or img.height > max_size:
                    img.thumbnail((max_size, max_size), Image.Resampling.LANCZOS)
                # 保存处理后的图片到内存中的BytesIO对象
                img.save(output, format=img.format)

            # 返回处理后的图片数据（bytes）
            return output.getvalue()

        except Exception as e:
            raise ValueError(f"图片压缩失败: {e}")

    @staticmethod
    async def download_image(url: str) -> bytes | None:
        """下载图片"""
        url = url.replace("https://", "http://")
        try:
            async with aiohttp.ClientSession() as client:
                response = await client.get(url)
                img_bytes = await response.read()
                return img_bytes
        except Exception as e:
            logger.error(f"图片下载失败: {e}")

    async def get_avatar(self, event: AstrMessageEvent, user_id: str) -> bytes | None:
        """下载头像（带缓存功能）"""
        # 如果缓存被禁用，直接下载
        if self._max_cache_size <= 0:
            logger.debug("头像缓存已禁用，直接下载")
        else:
            # 先尝试从缓存获取
            cached_avatar = self._get_cached_avatar(user_id)
            if cached_avatar:
                logger.debug(f"从缓存获取头像: {user_id}")
                return cached_avatar
        
        # 缓存中没有或缓存被禁用，下载头像
        if not user_id.isdigit():
            user_id = "".join(random.choices("0123456789", k=9))
        avatar_url = f"https://q4.qlogo.cn/headimg_dl?dst_uin={user_id}&spec=640"
        try:
            async with aiohttp.ClientSession() as client:
                response = await client.get(avatar_url, timeout=10)
                response.raise_for_status()
                avatar_data = await response.read()
                
                # 如果缓存未禁用，缓存头像数据
                if self._max_cache_size > 0:
                    self._cache_avatar(user_id, avatar_data)
                    logger.debug(f"下载并缓存头像: {user_id}")
                else:
                    logger.debug(f"下载头像（缓存已禁用）: {user_id}")
                
                return avatar_data
        except Exception as e:
            logger.error(f"下载头像失败: {e}")
            return None
