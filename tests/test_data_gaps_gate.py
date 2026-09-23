"""P1-1a/1b 数据门守卫：缺口清单计算 + prompt/报告落点存在性。"""
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from src.analyzer import _compute_data_gaps  # noqa: E402


def test_empty_context_means_fundamentals_and_news_gaps():
    assert _compute_data_gaps({}, None) == ["fundamentals", "news"]


def test_present_news_clears_news_gap():
    gaps = _compute_data_gaps(
        {"data_missing": True,
         "fundamental_context": {"earnings": {"data": {"financial_report": {"roe": "N/A"}}}}},
        "news text here",
    )
    assert gaps == ["technical", "fundamentals"]


def test_full_context_has_no_gaps():
    ctx = {"fundamental_context": {"earnings": {"data": {"financial_report": {"roe": "12.5"}}}}}
    assert _compute_data_gaps(ctx, "news text here") == []


def test_prompt_and_report_carry_the_gate():
    src_dir = pathlib.Path(__file__).resolve().parents[1] / "src"
    analyzer = (src_dir / "analyzer.py").read_text(encoding="utf-8")
    notification = (src_dir / "notification.py").read_text(encoding="utf-8")
    assert "数据缺口声明" in analyzer, "prompt 缺数据门声明块（P1-1a 回归）"
    assert "新闻维度直接跳过、不参与打分" in analyzer, "空新闻分支缺权重分摊（P1-1b 回归）"
    assert "数据缺口" in notification and "data_gaps" in notification, "报告详情段首缺缺口披露（P1-1a 回归）"
