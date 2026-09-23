# feishu_doc.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import importlib.util
import logging
import json
from typing import List, Dict, Any, Optional
from src.config import get_config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# lark-oapi SDK 惰性加载（P5-14）
# ---------------------------------------------------------------------------
# 为什么惰性化：`import lark_oapi` 会拉入整份生成式 API 面，冷启动实测 57–65 秒。
# 而本模块的唯一调用点 main.py 里 `from src.feishu_doc import FeishuDocManager` 位于
# 完成路径 try 的**顶部、无条件执行**（既不受 --no-notify 也不受 is_configured()
# 门禁），因此每一次 DSA 运行（包括我们流水线的 --no-notify 运行）都会实付这笔
# 冷启动代价。把真正的 import 推迟到首次需要 SDK 时即可消除该开销：未配置 /
# 未调用时零成本。
#
# `find_spec` 只**定位**包、不执行它，因此是廉价的可用性探针；真正的（昂贵）
# import 发生在 _load_lark_sdk() 首次被调用时。
FEISHU_DOC_SDK_AVAILABLE = importlib.util.find_spec("lark_oapi") is not None

# SDK 名字不在模块级预先定义：由 _load_lark_sdk() 首次成功导入后写入 globals()，
# 并由下方 PEP 562 的模块 __getattr__ 对外按需暴露。
_SDK_LOADED = False

# __getattr__ 允许惰性解析的名字白名单：
#  * `lark` —— 原先由 `import lark_oapi as lark` 带入，对外可能被引用；
#  * 其余类名 —— 原先由 `from lark_oapi.api.docx.v1 import *` 带入，本模块内部
#    实际用到（构建文档 / 构建 Block），一并按需暴露以免破坏属性访问。
_LAZY_SDK_NAMES = frozenset({
    "lark",
    "CreateDocumentRequest",
    "CreateDocumentRequestBody",
    "CreateDocumentBlockChildrenRequest",
    "CreateDocumentBlockChildrenRequestBody",
    "Block",
    "Divider",
    "TextRun",
    "TextElementStyle",
    "TextElement",
    "Text",
    "TextStyle",
})


def _load_lark_sdk() -> bool:
    """首次使用时导入 lark-oapi SDK。幂等且全有或全无。

    返回 True 表示 SDK 可用（`lark` 及各 docx 类名已就绪）。所有名字先收集到
    局部变量，待全部导入成功后再一次性提交到模块 globals()，因此部分失败绝不会
    留下半加载状态给后续调用者。
    """
    global lark, _SDK_LOADED
    global CreateDocumentRequest, CreateDocumentRequestBody
    global CreateDocumentBlockChildrenRequest, CreateDocumentBlockChildrenRequestBody
    global Block, Divider, TextRun, TextElementStyle, TextElement, Text, TextStyle

    if _SDK_LOADED:
        # `lark` 只在核心导入成功后才被赋值，因此它是比 find_spec 探针更准确的
        # "SDK 可用" 信号（用 `in globals()` 判定，避免导入失败时触发 NameError）。
        return "lark" in globals()
    _SDK_LOADED = True

    try:
        import lark_oapi as lark_module
        from lark_oapi.api.docx.v1 import (
            Block as _block,
            Divider as _divider,
            TextRun as _text_run,
            TextElementStyle as _text_element_style,
            TextElement as _text_element,
            Text as _text,
            TextStyle as _text_style,
            CreateDocumentRequest as _create_document_request,
            CreateDocumentRequestBody as _create_document_request_body,
            CreateDocumentBlockChildrenRequest as _create_document_block_children_request,
            CreateDocumentBlockChildrenRequestBody as _create_document_block_children_request_body,
        )
    except ImportError:
        return False

    lark = lark_module
    Block = _block
    Divider = _divider
    TextRun = _text_run
    TextElementStyle = _text_element_style
    TextElement = _text_element
    Text = _text
    TextStyle = _text_style
    CreateDocumentRequest = _create_document_request
    CreateDocumentRequestBody = _create_document_request_body
    CreateDocumentBlockChildrenRequest = _create_document_block_children_request
    CreateDocumentBlockChildrenRequestBody = _create_document_block_children_request_body
    return True


def __getattr__(name: str) -> Any:
    """按需解析惰性加载的 SDK 名字（供属性访问 / 打桩使用）。

    只处理显式白名单；其余名字照常抛 AttributeError，因此拼写错误会立刻失败，
    而不会触发一次毫无意义的 SDK 导入。SDK 不可用时该名字确实不存在，同样抛
    AttributeError（而不是从 globals 查找里泄漏 KeyError）。
    """
    if name in _LAZY_SDK_NAMES:
        _load_lark_sdk()
        try:
            return globals()[name]
        except KeyError:
            raise AttributeError(
                f"module {__name__!r} has no attribute {name!r}"
            ) from None
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


class FeishuDocManager:
    """飞书云文档管理器 (基于官方 SDK lark-oapi)"""

    def __init__(self):
        self.config = get_config()
        self.app_id = self.config.feishu_app_id
        self.app_secret = self.config.feishu_app_secret
        self.folder_token = self.config.feishu_folder_token

        # 初始化 SDK 客户端
        # SDK 会自动处理 tenant_access_token 的获取和刷新，无需人工干预
        # 仅在配置完整时才加载 SDK —— 未配置时连 import 都不触发（省下 57-65s）
        if self.is_configured() and _load_lark_sdk():
            self.client = lark.Client.builder() \
                .app_id(self.app_id) \
                .app_secret(self.app_secret) \
                .log_level(lark.LogLevel.INFO) \
                .build()
        else:
            self.client = None

    def is_configured(self) -> bool:
        """检查配置是否完整"""
        return bool(self.app_id and self.app_secret and self.folder_token)

    def create_daily_doc(self, title: str, content_md: str) -> Optional[str]:
        """
        创建日报文档
        """
        if not self.client or not self.is_configured():
            logger.warning("飞书 SDK 未初始化或配置缺失，跳过创建")
            return None

        # 确保 SDK 类名已就绪（幂等；通常 client 非空即代表已加载）。
        # 首次真正用到 SDK 的路径在此付一次性的 import 成本。
        if not _load_lark_sdk():
            logger.warning("lark-oapi 不可用，跳过创建飞书文档")
            return None

        try:
            # 1. 创建文档
            # 使用官方 SDK 的 Builder 模式构造请求
            create_request = CreateDocumentRequest.builder() \
                .request_body(CreateDocumentRequestBody.builder()
                              .folder_token(self.folder_token)
                              .title(title)
                              .build()) \
                .build()

            response = self.client.docx.v1.document.create(create_request)

            if not response.success():
                logger.error(f"创建文档失败: {response.code} - {response.msg} - {response.error}")
                return None

            doc_id = response.data.document.document_id
            # 这里的 domain 只是为了生成链接，实际访问会重定向
            doc_url = f"https://feishu.cn/docx/{doc_id}"
            logger.info(f"飞书文档创建成功: {title} (ID: {doc_id})")

            # 2. 解析 Markdown 并写入内容
            # 将 Markdown 转换为 SDK 需要的 Block 对象列表
            blocks = self._markdown_to_sdk_blocks(content_md)

            # 飞书 API 限制每次写入 Block 数量（建议 50 个左右），分批写入
            batch_size = 50
            doc_block_id = doc_id  # 文档本身也是一个 block

            for i in range(0, len(blocks), batch_size):
                batch_blocks = blocks[i:i + batch_size]

                # 构造批量添加块的请求
                batch_add_request = CreateDocumentBlockChildrenRequest.builder() \
                    .document_id(doc_id) \
                    .block_id(doc_block_id) \
                    .request_body(CreateDocumentBlockChildrenRequestBody.builder()
                                  .children(batch_blocks)  # SDK 需要 Block 对象列表
                                  .index(-1)  # 追加到末尾
                                  .build()) \
                    .build()

                write_resp = self.client.docx.v1.document_block_children.create(batch_add_request)

                if not write_resp.success():
                    logger.error(f"写入文档内容失败(批次{i}): {write_resp.code} - {write_resp.msg}")

            logger.info(f"文档内容写入完成")
            return doc_url

        except Exception as e:
            logger.error(f"飞书文档操作异常: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def _markdown_to_sdk_blocks(self, md_text: str) -> List[Block]:
        """
        将简单的 Markdown 转换为飞书 SDK 的 Block 对象
        """
        # 确保 SDK 类名已就绪（幂等）；正常情况下调用方 create_daily_doc 已加载。
        if not _load_lark_sdk():
            return []

        blocks = []
        lines = md_text.split('\n')

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # 默认普通文本 (Text = 2)
            block_type = 2
            text_content = line

            # 识别标题
            if line.startswith('# '):
                block_type = 3  # H1
                text_content = line[2:]
            elif line.startswith('## '):
                block_type = 4  # H2
                text_content = line[3:]
            elif line.startswith('### '):
                block_type = 5  # H3
                text_content = line[4:]
            elif line.startswith('---'):
                # 分割线
                blocks.append(Block.builder()
                              .block_type(22)
                              .divider(Divider.builder().build())
                              .build())
                continue

            # 构造 Text 类型的 Block
            # SDK 的结构嵌套比较深: Block -> Text -> elements -> TextElement -> TextRun -> content
            text_run = TextRun.builder() \
                .content(text_content) \
                .text_element_style(TextElementStyle.builder().build()) \
                .build()

            text_element = TextElement.builder() \
                .text_run(text_run) \
                .build()

            text_obj = Text.builder() \
                .elements([text_element]) \
                .style(TextStyle.builder().build()) \
                .build()

            # 根据 block_type 放入正确的属性容器
            block_builder = Block.builder().block_type(block_type)

            if block_type == 2:
                block_builder.text(text_obj)
            elif block_type == 3:
                block_builder.heading1(text_obj)
            elif block_type == 4:
                block_builder.heading2(text_obj)
            elif block_type == 5:
                block_builder.heading3(text_obj)

            blocks.append(block_builder.build())

        return blocks
