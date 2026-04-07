from __future__ import annotations

import sys
import threading
import traceback
import random
from functools import partial
from typing import Any, Callable
from dataclasses import replace
from datetime import datetime
from pathlib import Path

from bot_worker import BotWorker
from models import AccountConfig, AppSettings, BotEvent, BotSnapshot, apply_app_settings
from browser_profile import generate_browser_profile, is_complete_browser_profile
from registration import RegistrationResult, random_nickname, register_new_account_api_only
from siege_client import (
    get_bp_progress,
    get_inventory,
    get_player_progress,
    set_http_proxy,
    set_request_browser_profile,
)
from storage import (
    AccountManagerCacheStore,
    AccountStore,
    AppSettingsStore,
    ProxyPoolStore,
    StateStore,
    WalletStore,
    ensure_project_data_initialized,
)

try:
    from PySide6.QtCore import QEvent, QObject, QPoint, Qt, QTimer, Signal
    from PySide6.QtGui import QBrush, QColor, QFont
    from PySide6.QtWidgets import (
        QApplication,
        QButtonGroup,
        QCheckBox,
        QComboBox,
        QDialog,
        QFileDialog,
        QFormLayout,
        QFrame,
        QGridLayout,
        QGroupBox,
        QHBoxLayout,
        QLabel,
        QLineEdit,
        QListWidget,
        QListWidgetItem,
        QMainWindow,
        QMenu,
        QMessageBox,
        QHeaderView,
        QPlainTextEdit,
        QProgressBar,
        QProgressDialog,
        QAbstractItemView,
        QRadioButton,
        QPushButton,
        QScrollArea,
        QSizePolicy,
        QSpinBox,
        QStackedWidget,
        QStyleFactory,
        QTabWidget,
        QTableWidget,
        QTableWidgetItem,
        QVBoxLayout,
        QWidget,
    )
except Exception as e:  # pragma: no cover
    raise RuntimeError("Нужен PySide6: pip install PySide6") from e

APP_CLIENT_VERSION = "mngmv2a9"

THEME_QSS = """
QWidget {
    font-size: 13px;
    outline: none;
}
QMainWindow, QWidget#centralRoot {
    background-color: #0a0a0a;
    color: #f2f2f2;
}
QLabel { color: #f2f2f2; }
QLabel#sectionTitle {
    font-size: 12px;
    font-weight: 800;
    letter-spacing: 0.12em;
    color: #8e8e8e;
}
QFrame#sidebar {
    background-color: #101010;
    border: none;
    border-right: 1px solid #252525;
}
QFrame#surfacePanel {
    background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #1a1a1a, stop:1 #141414);
    border: 1px solid #2a2a2a;
    border-radius: 14px;
}
QFrame#surfaceLow {
    background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #161616, stop:1 #101010);
    border: 1px solid #2a2a2a;
    border-radius: 14px;
}
QScrollArea#fleetScroll {
    border: none;
    background: transparent;
}
QScrollArea#fleetScroll > QWidget > QWidget#fleetInner {
    background-color: #0a0a0a;
}
QScrollBar:vertical {
    background: #141414;
    width: 10px;
    margin: 0;
    border-radius: 5px;
}
QScrollBar::handle:vertical {
    background: #333333;
    min-height: 24px;
    border-radius: 5px;
}
QScrollBar::handle:vertical:hover { background: #404040; }
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height: 0; }
QFrame#metricTile {
    border-radius: 14px;
    border: 1px solid #2a2a2a;
}
QFrame#accountCard {
    background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #1c1c1c, stop:1 #141414);
    border: 1px solid #2d2d2d;
    border-radius: 14px;
}
QFrame#accountCard[accountBanned="true"] {
    background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #2c1618, stop:1 #1a0e10);
    border: 2px solid #c63e3e;
    border-radius: 14px;
}
QFrame#fleetShell {
    background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #161616, stop:1 #0e0e0e);
    border: 1px solid #2a2a2a;
    border-radius: 16px;
}
QFrame#fleetStrip {
    background-color: #141414;
    border: none;
    border-bottom: 1px solid #252525;
    border-top-left-radius: 15px;
    border-top-right-radius: 15px;
}
QFrame#accountActivity {
    background-color: rgba(0, 0, 0, 0.22);
    border: 1px solid #2a2a2a;
    border-radius: 10px;
}
QTableWidget {
    background-color: #1c1c1c;
    alternate-background-color: #232323;
    gridline-color: #3d3d3d;
    border: 1px solid #404040;
    border-radius: 10px;
    color: #eaeaea;
}
QTableWidget::item {
    padding: 4px 8px;
    border: none;
}
QTableWidget::item:selected {
    background-color: #2d4a6e;
    color: #ffffff;
}
QHeaderView::section {
    background-color: #2a2a2a;
    color: #e4e4e4;
    padding: 8px 10px;
    border: 1px solid #454545;
    font-weight: 700;
    font-size: 11px;
}
QHeaderView::section:vertical {
    background-color: #2a2a2a;
    color: #bcbcbc;
    border: 1px solid #454545;
    padding: 4px;
}
QTableCornerButton::section {
    background-color: #2a2a2a;
    border: 1px solid #454545;
}
QFrame#accountCardFooter {
    background-color: #141414;
    border: none;
    border-top: 1px solid #262626;
    border-bottom-left-radius: 13px;
    border-bottom-right-radius: 13px;
}
QPushButton#navBtn {
    color: #9a9a9a;
    text-align: left;
    padding: 14px 22px;
    border: none;
    border-radius: 0px;
    font-size: 11px;
    font-weight: 700;
    letter-spacing: 0.08em;
}
QPushButton#navBtn:checked {
    color: #3fff8b;
    background-color: #161616;
    border-right: 3px solid #3fff8b;
}
QPushButton#navBtn:hover:!checked {
    color: #f2f2f2;
    background-color: #161616;
}
QPushButton#deployBtn {
    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #4cff9a, stop:1 #1dce6c);
    color: #052e16;
    font-weight: 800;
    border-radius: 10px;
    padding: 14px 16px;
    border: 1px solid #2d5c40;
}
QPushButton#deployBtn:hover {
    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #6cffae, stop:1 #36e880);
}
QPushButton#deployBtn:pressed {
    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #34e87a, stop:1 #159a52);
}
QPushButton#primary {
    background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #4cff9a, stop:1 #2ee878);
    color: #052e16;
    font-weight: 800;
    border-radius: 10px;
    padding: 11px 20px;
    border: 1px solid #2d5c40;
}
QPushButton#primary:hover {
    background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #6cffae, stop:1 #48f090);
}
QPushButton#primary:pressed {
    background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #2ccd6f, stop:1 #22a85b);
}
QPushButton#secondary {
    background-color: #181818;
    color: #eaeaea;
    border: 1px solid #353535;
    border-radius: 10px;
    padding: 10px 18px;
    font-weight: 600;
}
QPushButton#secondary:hover {
    background-color: #222222;
    border: 1px solid #454545;
}
QPushButton#secondary:pressed { background-color: #121212; }
QPushButton#danger {
    color: #ffc8c4;
    background-color: #3d1818;
    border: 1px solid #7a2a2a;
    border-radius: 9px;
    padding: 11px 22px;
    font-weight: 700;
    font-size: 11px;
}
QPushButton#danger:hover { background-color: #4a2020; border-color: #933; }
QPushButton#danger:pressed { background-color: #321414; }
QPushButton#acctProxyBtn {
    background-color: #1f1f1f;
    color: #3fff8b;
    border: 1px solid #3a5a4a;
    border-radius: 6px;
    padding: 1px 8px;
    font-weight: 800;
    font-size: 10px;
    min-height: 0px;
    max-height: 24px;
}
QPushButton#acctProxyBtn:hover {
    background-color: #2a2a2a;
    border: 1px solid #4caf7a;
    color: #ffffff;
}
QPushButton#acctProxyBtn:pressed {
    background-color: #151515;
}
QPushButton#exportPresetBtn {
    background-color: #181818;
    color: #eaeaea;
    border: 1px solid #353535;
    border-radius: 10px;
    padding: 10px 18px;
    font-weight: 600;
}
QPushButton#exportPresetBtn:hover:!checked {
    background-color: #222222;
    border: 1px solid #454545;
}
QPushButton#exportPresetBtn:checked {
    background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #4cff9a, stop:1 #2ee878);
    color: #052e16;
    font-weight: 800;
    border: 1px solid #2d5c40;
}
QPushButton#exportPresetBtn:checked:hover {
    background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #6cffae, stop:1 #48f090);
}
QPushButton#exportPresetBtn:pressed:!checked { background-color: #121212; }
QPushButton#exportPresetBtn:checked:pressed {
    background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #2ccd6f, stop:1 #22a85b);
}
QPushButton#toolBtn {
    background-color: #1a1a1a;
    color: #d0d0d0;
    border: 1px solid #333333;
    border-radius: 8px;
    padding: 8px 14px;
    font-weight: 600;
    font-size: 12px;
    min-height: 16px;
}
QPushButton#toolBtn:hover {
    background-color: #242424;
    border: 1px solid #3a5a4a;
    color: #ffffff;
}
QPushButton#startBtn {
    background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #4cffa3, stop:1 #28dd72);
    color: #052e16;
    font-weight: 800;
    border-radius: 9px;
    padding: 9px 18px;
    border: 1px solid #2d6c45;
    font-size: 12px;
}
QPushButton#startBtn:hover {
    background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #6cffb5, stop:1 #48ec88);
}
QPushButton#startBtn:pressed {
    background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #26c868, stop:1 #1fa555);
}
QPushButton#stopAllBtn {
    background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #ff5454, stop:1 #d62828);
    color: #fff6f6;
    font-weight: 800;
    border-radius: 10px;
    padding: 11px 20px;
    border: 1px solid #8f2020;
}
QPushButton#stopAllBtn:hover {
    background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #ff7070, stop:1 #e63232);
    border: 1px solid #a52a2a;
}
QPushButton#stopAllBtn:pressed {
    background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #c42020, stop:1 #9e1818);
    color: #eeeeee;
}
QLineEdit, QPlainTextEdit, QSpinBox, QComboBox {
    background-color: #141414;
    border: 1px solid #333333;
    border-radius: 10px;
    padding: 10px 12px;
    color: #f2f2f2;
    selection-background-color: #3fff8b;
    selection-color: #052e16;
}
QLineEdit:focus, QPlainTextEdit:focus, QSpinBox:focus, QComboBox:focus {
    border: 1px solid #3fff8b;
}
QComboBox::drop-down {
    border: none;
    width: 22px;
}
QPlainTextEdit#eventLog {
    background-color: #060606;
    color: #c8c8c8;
    border: 1px solid #282828;
    border-radius: 12px;
    padding: 12px 14px;
    font-family: "Cascadia Mono", "Consolas", monospace;
}
QListWidget {
    background-color: #141414;
    border: 1px solid #2a2a2a;
    border-radius: 12px;
    color: #e4e4e4;
    padding: 6px;
}
QListWidget::item {
    padding: 8px 10px;
    border-radius: 8px;
}
QListWidget::item:selected {
    background: #1f3d2a;
    color: #3fff8b;
}
QListWidget::item:hover:!selected {
    background: #1e1e1e;
}
QTabWidget::pane {
    border: 1px solid #2a2a2a;
    border-radius: 10px;
    top: -1px;
    padding: 4px;
    background: #121212;
}
QTabBar::tab {
    background: #141414;
    color: #888888;
    padding: 10px 18px;
    margin-right: 4px;
    border-top-left-radius: 10px;
    border-top-right-radius: 10px;
    font-weight: 600;
}
QTabBar::tab:selected {
    color: #3fff8b;
    background: #1a1a1a;
    border: 1px solid #2a2a2a;
    border-bottom: none;
}
QProgressBar {
    border: none;
    background: #242424;
    border-radius: 5px;
    height: 7px;
    text-align: center;
}
QProgressBar::chunk {
    background-color: #3fff8b;
    border-radius: 5px;
}
QCheckBox { color: #eaeaea; spacing: 10px; }
QCheckBox::indicator {
    width: 18px;
    height: 18px;
    border-radius: 4px;
    border: 1px solid #444444;
    background: #141414;
}
QCheckBox::indicator:checked {
    background: #3fff8b;
    border: 1px solid #2a5c3e;
}
QDialog {
    background-color: #0e0e0e;
    color: #eaeaea;
}
QDialog QLabel {
    color: #eaeaea;
}
QDialog QRadioButton {
    color: #eaeaea;
    spacing: 10px;
    font-size: 13px;
}
QDialog QRadioButton::indicator {
    width: 18px;
    height: 18px;
    border-radius: 4px;
    border: 1px solid #555555;
    background: #1a1a1a;
}
QDialog QRadioButton::indicator:checked {
    background: #3fff8b;
    border: 1px solid #2a5c3e;
}
QDialog QGroupBox {
    color: #e4e4e4;
    font-weight: 700;
    border: 1px solid #3a3a3a;
    border-radius: 10px;
    margin-top: 10px;
    padding-top: 14px;
}
QDialog QGroupBox::title {
    subcontrol-origin: margin;
    left: 12px;
    padding: 0 6px;
}
"""


def format_ch_amount(n: object) -> str:
    """Читаемое отображение количества $SIEGE."""
    if n is None:
        return "—"
    try:
        x = float(n)
        s = f"{x:.8f}".rstrip("0").rstrip(".")
        return s if s else "0"
    except (TypeError, ValueError):
        return str(n)


def _short_proxy_cell(px: str, max_len: int = 28) -> str:
    if not px:
        return "—"
    if len(px) <= max_len:
        return px
    return px[: max_len - 1] + "…"


def _short_sol_addr(addr: str, left: int = 6, right: int = 4) -> str:
    if not addr:
        return "—"
    if len(addr) <= left + right + 3:
        return addr
    return f"{addr[:left]}…{addr[-right:]}"


def _short_account_id_cell(aid: str, head: int = 10) -> str:
    if not aid:
        return "—"
    if len(aid) <= head + 1:
        return aid
    return f"{aid[:head]}…"


def _export_template_format(tpl: str, row: dict[str, Any]) -> str:
    s = tpl
    for k, v in row.items():
        s = s.replace("{" + k + "}", "" if v is None else str(v))
    return s


def _wave_done_activity_text(payload: dict) -> str:
    """Текст блока активности на карточке после победы в волне."""
    wn = payload.get("wave")
    boss = " · босс" if payload.get("is_boss") else ""
    tr = payload.get("token_reward")
    xp = payload.get("xp_reward")
    sc = payload.get("scrap_reward")
    bal = payload.get("balance")
    return (
        f"Победа в волне {wn}{boss}\n"
        f"  +{format_ch_amount(tr)} $SIEGE   ·   +{xp if xp is not None else 0} XP   ·   "
        f"+{sc if sc is not None else 0} лом\n"
        f"  Баланс  {format_ch_amount(bal)} $SIEGE"
    )


def _fetch_account_details_dict(acc: AccountConfig, *, account_banned: bool = False) -> dict:
    """Сетевые вызовы — только из фонового потока (не из GUI)."""
    if account_banned:
        return {"equip": "ACCOUNT_BANNED — опрос API отключён"}
    try:
        set_http_proxy(acc.http_proxy)
        prof = (
            acc.browser_profile
            if is_complete_browser_profile(acc.browser_profile)
            else generate_browser_profile()
        )
        set_request_browser_profile(prof)
        prog = get_player_progress(acc.bearer_token, acc.client_version)
        comp = prog.get("computed", {})
        inv = get_inventory(acc.bearer_token, acc.client_version)
        eq = [i for i in inv if isinstance(i, dict) and i.get("is_equipped")]
        eq_txt = [f"{i.get('equipped_slot') or i.get('item_type')}:{i.get('rarity')}" for i in eq]
        bp = get_bp_progress(acc.bearer_token, acc.client_version)
        return {
            "level": prog.get("level"),
            "hp": comp.get("max_hp"),
            "dmg": comp.get("damage"),
            "crit": comp.get("crit_chance"),
            "speed": comp.get("attack_speed_ms"),
            "equip": " | ".join(eq_txt) if eq_txt else "нет экипа",
            "bp_tier": bp.get("current_tier"),
            "bp_xp": bp.get("current_xp"),
            "bp_free_claimed": len(bp.get("claimed_free") or []),
        }
    except Exception as e:
        return {"equip": f"ошибка: {e}"}
    finally:
        set_request_browser_profile(None)


def _hand_cursor_widgets(root: QWidget) -> None:
    for btn in root.findChildren(QPushButton):
        btn.setCursor(Qt.CursorShape.PointingHandCursor)
    for cb in root.findChildren(QCheckBox):
        cb.setCursor(Qt.CursorShape.PointingHandCursor)


def normalize_proxy_input(raw: str | None) -> str | None:
    if raw is None:
        return None
    s = str(raw)
    s = s.replace("\r", "").replace("\n", "").replace("\t", "")
    s = s.replace(";", ":").replace(",", ":").replace("：", ":")
    s = s.strip().strip("'").strip('"')
    if not s:
        return None
    if " " in s and ":" not in s:
        s = ":".join([p for p in s.split() if p])
    else:
        s = s.replace(" ", "")
    return s or None


def _unique_lines_by_proxy_norm(raw_lines: list[str]) -> list[str]:
    """Порядок как в пуле, без дубликатов по нормализованному прокси."""
    seen: set[str] = set()
    out: list[str] = []
    for line in raw_lines:
        n = normalize_proxy_input(line)
        if not n or n in seen:
            continue
        seen.add(n)
        out.append(str(line).strip())
    return out


def occupied_proxy_norms_from_accounts(accounts: list[AccountConfig]) -> set[str]:
    s: set[str] = set()
    for a in accounts:
        n = normalize_proxy_input(a.http_proxy)
        if n:
            s.add(n)
    return s


def count_free_proxies_in_pool(proxy_pool: ProxyPoolStore, occupied: set[str]) -> int:
    nfree = 0
    for line in proxy_pool.list_all():
        nn = normalize_proxy_input(line)
        if nn and nn not in occupied:
            nfree += 1
    return nfree


class WorkerBridge(QObject):
    event_signal = Signal(object)
    log_signal = Signal(str, str)


def _metric_card(
    icon_char: str,
    value: str,
    label: str,
    accent: str,
    badge: str = "",
    *,
    value_fixed_width: int | None = None,
    value_monospace: bool = False,
) -> tuple[QFrame, QLabel]:
    card = QFrame()
    card.setObjectName("metricTile")
    card.setStyleSheet(
        f"QFrame#metricTile {{"
        f"background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #1e1e1e, stop:1 #141414);"
        f"border: 1px solid #2c2c2c;"
        f"border-left: 4px solid {accent};"
        f"border-radius: 14px;"
        f"}}"
    )
    lay = QVBoxLayout(card)
    lay.setContentsMargins(22, 20, 22, 20)
    lay.setSpacing(6)
    top = QHBoxLayout()
    ic = QLabel(icon_char)
    ic.setStyleSheet(f"font-size: 26px; color: {accent}; background: transparent;")
    top.addWidget(ic)
    if badge:
        b = QLabel(badge)
        b.setStyleSheet(
            f"font-size: 9px; font-weight: 800; color: {accent}; "
            f"background-color: rgba(255, 255, 255, 0.05); padding: 5px 10px; border-radius: 12px;"
            f"border: 1px solid rgba(255, 255, 255, 0.08);"
        )
        top.addStretch(1)
        top.addWidget(b)
    else:
        top.addStretch(1)
    lay.addLayout(top)
    v = QLabel(value)
    v_style = "font-size: 30px; font-weight: 900; color: #ffffff; letter-spacing: -0.02em;"
    if value_monospace:
        v_style = (
            "font-size: 28px; font-weight: 900; color: #ffffff; "
            "font-family: 'Cascadia Mono', Consolas, 'Courier New', monospace; letter-spacing: 0.02em;"
        )
    v.setStyleSheet(v_style)
    if value_fixed_width is not None:
        v.setFixedWidth(value_fixed_width)
        v.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
    lay.addWidget(v)
    sub = QLabel(label.upper())
    sub.setStyleSheet("font-size: 10px; font-weight: 800; color: #808080; letter-spacing: 0.1em;")
    lay.addWidget(sub)
    return card, v


def _stat_chip(text: str) -> QLabel:
    chip = QLabel(text)
    chip.setStyleSheet(
        "QLabel { background-color: #152018; color: #8fd4a8; font-size: 11px; font-weight: 700; "
        "padding: 5px 11px; border-radius: 8px; border: 1px solid #2a4a36; }"
    )
    return chip


def _stat_chip_ch(text: str) -> QLabel:
    chip = QLabel(text)
    chip.setStyleSheet(
        "QLabel { background-color: #151d2e; color: #9cb8ff; font-size: 11px; font-weight: 700; "
        "padding: 5px 11px; border-radius: 8px; border: 1px solid #2a3d6e; }"
    )
    return chip


class AccountCard(QFrame):
    start_clicked = Signal(str)
    stop_clicked = Signal(str)
    refresh_clicked = Signal(str)

    def __init__(self, account: AccountConfig, parent=None) -> None:
        super().__init__(parent)
        self.account_id = account.account_id
        self.setObjectName("accountCard")
        self.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Preferred)
        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        body = QWidget()
        bl = QVBoxLayout(body)
        bl.setContentsMargins(22, 20, 22, 14)
        bl.setSpacing(14)

        row = QHBoxLayout()
        row.setSpacing(18)
        self._icon = QLabel("◆")
        self._icon.setFixedSize(52, 52)
        self._icon.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._icon.setStyleSheet(
            "background-color: rgba(63, 255, 139, 0.1); color: #3fff8b; "
            "border-radius: 14px; font-size: 22px; border: 1px solid #2a4d38;"
        )
        mid = QVBoxLayout()
        mid.setSpacing(8)
        title_row = QHBoxLayout()
        title_row.setSpacing(12)
        self.lbl_title = QLabel(f"{account.name}")
        self.lbl_title.setWordWrap(True)
        self.lbl_title.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Preferred)
        self.lbl_title.setStyleSheet("font-weight: 800; font-size: 17px; color: #ffffff; letter-spacing: -0.02em;")
        self.lbl_state = QLabel("OFF")
        self.lbl_state.setStyleSheet(
            "font-weight: 800; font-size: 10px; color: #888888; letter-spacing: 0.08em; "
            "background-color: #252525; padding: 4px 10px; border-radius: 8px; border: 1px solid #353535;"
        )
        title_row.addWidget(self.lbl_title, 0, Qt.AlignmentFlag.AlignVCenter)
        title_row.addWidget(self.lbl_state, 0, Qt.AlignmentFlag.AlignVCenter)
        title_row.addStretch(1)
        mid.addLayout(title_row)
        self.lbl_sub = QLabel("—")
        self.lbl_sub.setWordWrap(True)
        self.lbl_sub.setMinimumWidth(0)
        self.lbl_sub.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        self.lbl_sub.setStyleSheet(
            "color: #a8a8a8; font-size: 12px; font-weight: 500; line-height: 1.35;"
        )
        activity = QFrame()
        activity.setObjectName("accountActivity")
        al = QVBoxLayout(activity)
        al.setContentsMargins(12, 10, 12, 10)
        al.setSpacing(0)
        al.addWidget(self.lbl_sub)
        mid.addWidget(activity)
        right_col = QVBoxLayout()
        right_col.setSpacing(4)
        self.lbl_uptime = QLabel("")
        self.lbl_uptime.setStyleSheet("font-size: 10px; color: #555555;")
        self.lbl_uptime.setAlignment(Qt.AlignmentFlag.AlignRight)
        right_col.addWidget(self.lbl_uptime)
        right_col.addStretch(1)
        row.addWidget(self._icon)
        row.addLayout(mid, 1)
        row.addLayout(right_col)
        bl.addLayout(row)

        chips_row = QHBoxLayout()
        chips_row.setSpacing(10)
        self.chip_lvl = _stat_chip("Lv —")
        self.chip_ch = _stat_chip_ch("$SIEGE —")
        for c in (self.chip_lvl, self.chip_ch):
            chips_row.addWidget(c)
        chips_row.addStretch(1)
        bl.addLayout(chips_row)

        self._bar = QProgressBar()
        self._bar.setTextVisible(False)
        self._bar.setFixedHeight(6)
        self._bar.setMaximum(100)
        self._bar.setValue(0)
        bl.addWidget(self._bar)

        self.lbl_bp = QLabel("")
        self.lbl_bp.setWordWrap(True)
        self.lbl_bp.setMinimumWidth(0)
        self.lbl_bp.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Preferred)
        self.lbl_bp.setStyleSheet("font-size: 11px; color: #7a7a7a;")
        self.lbl_equip = QLabel("")
        self.lbl_equip.setWordWrap(True)
        self.lbl_equip.setMinimumWidth(0)
        self.lbl_equip.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Preferred)
        self.lbl_equip.setStyleSheet("font-size: 11px; color: #7a7a7a;")
        self.lbl_error = QLabel("")
        self.lbl_error.setWordWrap(True)
        self.lbl_error.setMinimumWidth(0)
        self.lbl_error.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Preferred)
        self.lbl_error.setStyleSheet("color: #ff8a84; font-size: 12px;")
        bl.addWidget(self.lbl_bp)
        bl.addWidget(self.lbl_equip)
        bl.addWidget(self.lbl_error)
        root.addWidget(body)

        footer = QFrame()
        footer.setObjectName("accountCardFooter")
        fl = QHBoxLayout(footer)
        fl.setContentsMargins(20, 12, 20, 14)
        fl.setSpacing(10)
        self.btn_start = QPushButton("Старт")
        self.btn_stop = QPushButton("Стоп")
        self.btn_refresh = QPushButton("Обновить")
        self.btn_start.setObjectName("startBtn")
        for b in (self.btn_stop, self.btn_refresh):
            b.setObjectName("toolBtn")
        self.btn_start.clicked.connect(lambda: self.start_clicked.emit(self.account_id))
        self.btn_stop.clicked.connect(lambda: self.stop_clicked.emit(self.account_id))
        self.btn_refresh.clicked.connect(lambda: self.refresh_clicked.emit(self.account_id))
        fl.addWidget(self.btn_start)
        fl.addWidget(self.btn_stop)
        fl.addWidget(self.btn_refresh)
        fl.addStretch(1)
        root.addWidget(footer)

    def update_card(self, account: AccountConfig, snap: BotSnapshot, details: dict) -> None:
        self.lbl_title.setText(f"{account.name}")
        last_msg = (snap.last_message or "").strip() or "Ожидание…"
        self.lbl_sub.setText(last_msg)
        running = snap.running

        self.setProperty("accountBanned", snap.account_banned)
        self.style().unpolish(self)
        self.style().polish(self)

        def _pill(text: str, fg: str, bg: str, border: str) -> None:
            self.lbl_state.setText(text)
            self.lbl_state.setStyleSheet(
                f"font-weight: 800; font-size: 10px; color: {fg}; letter-spacing: 0.08em; "
                f"background-color: {bg}; padding: 4px 10px; border-radius: 8px; border: 1px solid {border};"
            )

        if snap.account_banned:
            _pill("БАН", "#ff9a96", "#3a1618", "#8b3030")
            self._icon.setStyleSheet(
                "background-color: rgba(220, 62, 62, 0.18); color: #ff6b66; "
                "border-radius: 14px; font-size: 22px; border: 1px solid #8b2a2a;"
            )
        elif snap.last_error:
            _pill("СБОЙ", "#ff8a84", "#2a1818", "#5c3030")
            self._icon.setStyleSheet(
                "background-color: rgba(255, 113, 108, 0.12); color: #ff716c; "
                "border-radius: 14px; font-size: 22px; border: 1px solid #5c2a2a;"
            )
        elif running:
            _pill("В СЕТИ", "#3fff8b", "#14291c", "#2a4d38")
            self._icon.setStyleSheet(
                "background-color: rgba(63, 255, 139, 0.14); color: #3fff8b; "
                "border-radius: 14px; font-size: 22px; border: 1px solid #2a4d38;"
            )
        else:
            _pill("СТОП", "#9a9a9a", "#252525", "#353535")
            self._icon.setStyleSheet(
                "background-color: rgba(110, 155, 255, 0.1); color: #6e9bff; "
                "border-radius: 14px; font-size: 22px; border: 1px solid #2a3d5c;"
            )

        self.lbl_uptime.setText(str(snap.updated_at)[:19] if snap.updated_at else "")
        self.chip_lvl.setText(
            f"Lv {details.get('level', snap.level if snap.level is not None else '—')}"
        )
        bal = snap.balance
        self.chip_ch.setText(f"$SIEGE {format_ch_amount(bal)}")

        self.btn_start.setEnabled(not running and not snap.account_banned)
        self.btn_stop.setEnabled(running)

        w = snap.wave or 0
        self._bar.setValue(min(100, (w % 100) or (50 if running else 0)))
        if snap.account_banned:
            self._bar.setStyleSheet("QProgressBar::chunk { background-color: #d04444; }")
        elif snap.last_error:
            self._bar.setStyleSheet("QProgressBar::chunk { background-color: #ff716c; }")
        elif running:
            self._bar.setStyleSheet("QProgressBar::chunk { background-color: #3fff8b; }")
        else:
            self._bar.setStyleSheet("QProgressBar::chunk { background-color: #6e9bff; }")

        self.lbl_bp.setText(
            f"Battle Pass · tier {details.get('bp_tier', '—')} · XP {details.get('bp_xp', '—')} · "
            f"награды {details.get('bp_free_claimed', '—')}"
        )
        self.lbl_equip.setText(f"Экипировка · {details.get('equip', '—')}")
        self.lbl_error.setText(snap.last_error or "")


class AddAccountDialog(QDialog):
    BULK_PREVIEW_ROW_HEIGHT = 46

    def __init__(self, free_proxy_count: int, pool_total_lines: int, parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Добавить аккаунт")
        self.resize(720, 620)
        self._free = free_proxy_count
        self._pool_total = pool_total_lines
        self._unused_lines_fn: Callable[[], list[str]] | None = None
        self._occupied_norms_fn: Callable[[], set[str]] | None = None
        self._bulk_result: list[tuple[str, str]] | None = None

        self.tabs = QTabWidget(self)
        self.manual_tab = QWidget()
        self.reg_tab = QWidget()
        self.bulk_tab = QWidget()
        self.tabs.addTab(self.manual_tab, "Ручной token")
        self.tabs.addTab(self.reg_tab, "Авторегистрация")
        self.tabs.addTab(self.bulk_tab, "Массовая регистрация")

        man_outer = QVBoxLayout(self.manual_tab)
        man_outer.setContentsMargins(0, 0, 0, 0)
        man_form = QFormLayout()
        self.manual_name = QLineEdit(self.manual_tab)
        self.manual_token = QLineEdit(self.manual_tab)
        self.manual_proxy = QLineEdit(self.manual_tab)
        self.manual_token.setPlaceholderText("JWT без слова Bearer — только eyJ…")
        self.manual_proxy.setPlaceholderText("host:port:user:pass")
        man_form.addRow("Имя", self.manual_name)
        man_form.addRow("Bearer token", self.manual_token)
        man_form.addRow("Прокси", self.manual_proxy)
        self.lbl_manual_proxy_hint = QLabel(self._manual_proxy_caption())
        self.lbl_manual_proxy_hint.setStyleSheet("color: #adaaaa; font-size: 11px;")
        self.lbl_manual_proxy_hint.setWordWrap(True)
        man_form.addRow(self.lbl_manual_proxy_hint)
        man_outer.addLayout(man_form)
        man_outer.addStretch(1)
        self.lbl_manual_bearer_help = QLabel(
            "<b>Где взять Bearer token</b><br><br>"
            "Нужен аккаунт в игре <b>SolSiege</b> в браузере — зайди под тем персонажем, которого хочешь добавить "
            "в ферму.<br><br>"
            "1. Открой сайт игры и нажми <b>F12</b> (инструменты разработчика).<br>"
            "2. Вкладка <b>Сеть</b> (Network). При необходимости обнови страницу (F5) или сделай любое действие в игре, "
            "чтобы пошли запросы.<br>"
            "3. Отфильтруй запросы по <b>Fetch</b> / <b>XHR</b> или найди обращения к API игры.<br>"
            "4. Выбери любой запрос к серверу игры → раздел <b>Заголовки</b> (Headers).<br>"
            "5. Найди заголовок <b>Authorization</b>. Там будет что-то вроде "
            "<code>Bearer eyJhbGciOi…</code>. Скопируй <b>только JWT</b> — длинную часть <i>после</i> слова Bearer "
            "(само слово Bearer в поле вводить не нужно).<br><br>"
            "<b>Другой вариант:</b> вкладка <b>Приложение</b> (Application) → <b>Local Storage</b> или "
            "<b>Session Storage</b> для сайта игры — иногда токен лежит в ключах вроде token / accessToken "
            "(зависит от клиента).<br><br>"
            "<span style='color:#b08080;'>Токен даёт доступ к аккаунту — не отправляй никому и не свети в стримах.</span>"
        )
        self.lbl_manual_bearer_help.setWordWrap(True)
        self.lbl_manual_bearer_help.setTextFormat(Qt.TextFormat.RichText)
        self.lbl_manual_bearer_help.setStyleSheet("color: #9a9a9a; font-size: 11px;")
        man_outer.addWidget(self.lbl_manual_bearer_help)

        reg_layout = QVBoxLayout(self.reg_tab)
        reg_form = QFormLayout()
        self.reg_proxy = QLineEdit(self.reg_tab)
        self.reg_proxy.setPlaceholderText("host:port:user:pass или кнопка «Из пула»")
        self.reg_nick = QLineEdit(self.reg_tab)
        self.reg_nick.setText(random_nickname())
        self.reg_class = QComboBox(self.reg_tab)
        self.reg_class.addItems(["mage", "warrior", "archer"])
        btn_row = QHBoxLayout()
        self.btn_from_pool = QPushButton("Взять прокси из пула")
        self.btn_from_pool.setObjectName("secondary")
        btn_row.addWidget(self.btn_from_pool)
        btn_row.addStretch(1)
        reg_form.addRow("Прокси", self.reg_proxy)
        reg_form.addRow("Ник", self.reg_nick)
        reg_form.addRow("Класс", self.reg_class)
        reg_layout.addLayout(reg_form)
        reg_layout.addLayout(btn_row)
        self.lbl_pool_hint = QLabel(self._reg_pool_caption())
        self.lbl_pool_hint.setStyleSheet("color: #adaaaa; font-size: 11px;")
        self.lbl_pool_hint.setWordWrap(True)
        reg_layout.addWidget(self.lbl_pool_hint)

        bulk_outer = QVBoxLayout(self.bulk_tab)
        bulk_outer.setContentsMargins(0, 0, 0, 0)
        bulk_form = QFormLayout()
        bulk_count_row = QHBoxLayout()
        self.bulk_count = QSpinBox(self.bulk_tab)
        mx = max(1, free_proxy_count)
        self.bulk_count.setRange(1, mx)
        self.bulk_count.setValue(min(5, mx) if free_proxy_count else 1)
        self.btn_bulk_max = QPushButton("Max")
        self.btn_bulk_max.setObjectName("secondary")
        self.btn_bulk_max.setToolTip("Максимум аккаунтов под свободные прокси")
        self.btn_bulk_max.clicked.connect(self._bulk_set_max)
        bulk_count_row.addWidget(self.bulk_count)
        bulk_count_row.addWidget(self.btn_bulk_max)
        bulk_count_row.addStretch(1)
        self.bulk_proxy_mode = QComboBox(self.bulk_tab)
        self.bulk_proxy_mode.addItems(["По порядку в списке пула", "Случайно из пула"])
        self.bulk_proxy_mode.setToolTip(
            "По порядку — как строки идут в «Прокси-пул». Случайно — каждый раз новый случайный набор свободных прокси "
            "(набор обновляется при смене числа аккаунтов или этого режима)."
        )
        self.bulk_class = QComboBox(self.bulk_tab)
        self.bulk_class.addItems(["mage", "warrior", "archer"])
        self.bulk_autostart = QCheckBox("Запустить ботов сразу после регистрации")
        self.bulk_autostart.setChecked(False)
        w_bulk_count = QWidget()
        w_bulk_count.setLayout(bulk_count_row)
        bulk_form.addRow("Сколько аккаунтов", w_bulk_count)
        bulk_form.addRow("Прокси из пула", self.bulk_proxy_mode)
        bulk_form.addRow("Класс", self.bulk_class)
        bulk_form.addRow(self.bulk_autostart)
        bulk_outer.addLayout(bulk_form, 0)
        self.lbl_bulk_pool = QLabel(self._bulk_pool_caption())
        self.lbl_bulk_pool.setStyleSheet("color: #adaaaa; font-size: 11px;")
        self.lbl_bulk_pool.setWordWrap(True)
        bulk_outer.addWidget(self.lbl_bulk_pool)
        prev_lbl = QLabel(
            "Предпросмотр — сгенерированные ники можно заменить на более «человеческие» "
            "(латиница, цифры, подчёркивание, до 16 символов). По клику на прокси — выбор строки из пула."
        )
        prev_lbl.setStyleSheet("color: #8a8a8a; font-size: 11px;")
        prev_lbl.setWordWrap(True)
        bulk_outer.addWidget(prev_lbl, 0)
        self.bulk_preview_table = QTableWidget(0, 3, self.bulk_tab)
        self.bulk_preview_table.setHorizontalHeaderLabels(["№", "Никнейм", "Прокси — нажми, чтобы выбрать"])
        self.bulk_preview_table.verticalHeader().setVisible(False)
        vh_prev = self.bulk_preview_table.verticalHeader()
        vh_prev.setDefaultSectionSize(self.BULK_PREVIEW_ROW_HEIGHT)
        self.bulk_preview_table.setSelectionMode(QAbstractItemView.SelectionMode.NoSelection)
        self.bulk_preview_table.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        self.bulk_preview_table.setShowGrid(True)
        pol = QSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        pol.setVerticalStretch(1)
        self.bulk_preview_table.setSizePolicy(pol)
        self.bulk_preview_table.setMinimumHeight(260)
        bh = self.bulk_preview_table.horizontalHeader()
        bh.setSectionResizeMode(0, QHeaderView.ResizeMode.Fixed)
        self.bulk_preview_table.setColumnWidth(0, 44)
        bh.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        bh.setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        bh.setMinimumSectionSize(80)
        bulk_outer.addWidget(self.bulk_preview_table, 1)
        bulk_pol = QSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Expanding)
        self.bulk_tab.setSizePolicy(bulk_pol)
        self.bulk_count.valueChanged.connect(lambda _v: self._bulk_refresh_preview())
        self.bulk_proxy_mode.currentIndexChanged.connect(lambda _i: self._bulk_refresh_preview())
        self.tabs.currentChanged.connect(self._on_add_dialog_tab_changed)

        btns = QHBoxLayout()
        self.btn_add = QPushButton("ОК")
        self.btn_add.setObjectName("primary")
        self.btn_cancel = QPushButton("Отмена")
        self.btn_cancel.setObjectName("secondary")
        self.btn_cancel.clicked.connect(self.reject)
        self.btn_add.clicked.connect(self._try_accept)
        btns.addStretch(1)
        btns.addWidget(self.btn_add)
        btns.addWidget(self.btn_cancel)
        root = QVBoxLayout(self)
        root.setContentsMargins(16, 16, 16, 16)
        root.setSpacing(12)
        self.tabs.setSizePolicy(
            QSizePolicy.Policy.Expanding,
            QSizePolicy.Policy.Expanding,
        )
        root.addWidget(self.tabs, 1)
        root.addLayout(btns)
        _hand_cursor_widgets(self)

    def _on_add_dialog_tab_changed(self, idx: int) -> None:
        if idx == 2:
            self._bulk_refresh_preview()

    def _manual_proxy_caption(self) -> str:
        return (
            f"Один прокси — один аккаунт. Свободных для новой привязки: {self._free} "
            f"(строк в пуле: {self._pool_total}; занятые на аккаунтах уже вычтены)."
        )

    def _reg_pool_caption(self) -> str:
        return (
            f"Свободных прокси под регистрацию: {self._free}. В списке пула строк: {self._pool_total}. "
            "Из пула подставляется только прокси, ещё не закреплённый ни за одним аккаунтом."
        )

    def _bulk_pool_caption(self) -> str:
        return (
            f"За один прогон создаётся не больше числа свободных прокси. Сейчас свободно: {self._free}, "
            f"в пуле строк: {self._pool_total}."
        )

    def _bulk_set_max(self) -> None:
        self.bulk_count.setValue(max(1, self._free))

    def set_proxy_availability(self, free: int, pool_total: int) -> None:
        self._free = free
        self._pool_total = pool_total
        self.lbl_manual_proxy_hint.setText(self._manual_proxy_caption())
        self.lbl_pool_hint.setText(self._reg_pool_caption())
        self.lbl_bulk_pool.setText(self._bulk_pool_caption())
        mx = max(1, free)
        self.bulk_count.setMaximum(mx)
        if self.bulk_count.value() > mx:
            self.bulk_count.setValue(mx)
        self._bulk_refresh_preview()

    def attach_bulk_preview(
        self,
        unused_lines_fn: Callable[[], list[str]],
        occupied_norms_fn: Callable[[], set[str]],
    ) -> None:
        self._unused_lines_fn = unused_lines_fn
        self._occupied_norms_fn = occupied_norms_fn
        self._bulk_refresh_preview()

    def _try_accept(self) -> None:
        self._bulk_result = None
        if self.mode() == "bulk":
            rows = self._validate_bulk_rows()
            if rows is None:
                return
            self._bulk_result = rows
        self.accept()

    def _bulk_refresh_preview(self) -> None:
        if self._unused_lines_fn is None or not hasattr(self, "bulk_preview_table"):
            return
        n = self.bulk_count.value()
        pool_u = _unique_lines_by_proxy_norm(self._unused_lines_fn())
        random_mode = self.bulk_proxy_mode.currentIndex() == 1
        if random_mode:
            if n <= len(pool_u):
                chosen = random.sample(pool_u, n)
            else:
                chosen = list(pool_u)
        else:
            chosen = pool_u[:n]
        while len(chosen) < n:
            chosen.append("")
        self.bulk_preview_table.setRowCount(n)
        rh = self.BULK_PREVIEW_ROW_HEIGHT
        table_min_h = min(520, max(260, n * rh + self.bulk_preview_table.horizontalHeader().height() + 4))
        self.bulk_preview_table.setMinimumHeight(table_min_h)
        for i in range(n):
            it_n = QTableWidgetItem(str(i + 1))
            it_n.setFlags(Qt.ItemFlag.ItemIsEnabled)
            it_n.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            self.bulk_preview_table.setItem(i, 0, it_n)
            old_nick = ""
            w_old = self.bulk_preview_table.cellWidget(i, 1)
            if isinstance(w_old, QLineEdit):
                old_nick = w_old.text().strip()
            le = QLineEdit(old_nick or random_nickname())
            le.setMaxLength(16)
            le.setPlaceholderText("ник, латиница 0-9_")
            le.setMinimumHeight(rh - 8)
            self.bulk_preview_table.setCellWidget(i, 1, le)
            px = chosen[i] if i < len(chosen) else ""
            self._bulk_set_proxy_cell(i, px)
            self.bulk_preview_table.setRowHeight(i, rh)

    def _bulk_set_proxy_cell(self, row: int, proxy_line: str) -> None:
        p = (proxy_line or "").strip()
        lab = _short_proxy_cell(p, 48) if p else "— нажми, выбрать из пула —"
        btn = QPushButton(lab)
        btn.setProperty("proxy_full", p)
        btn.setToolTip(p or "Выбери свободную строку из пула")
        btn.setObjectName("secondary")
        btn.setMinimumHeight(self.BULK_PREVIEW_ROW_HEIGHT - 8)
        btn.clicked.connect(partial(self._bulk_open_proxy_menu, row))
        self.bulk_preview_table.setCellWidget(row, 2, btn)

    def _bulk_open_proxy_menu(self, row: int) -> None:
        btn = self.bulk_preview_table.cellWidget(row, 2)
        if not isinstance(btn, QPushButton):
            return
        cur = str(btn.property("proxy_full") or "")
        menu = QMenu(self)
        pool_lines = _unique_lines_by_proxy_norm(self._unused_lines_fn() if self._unused_lines_fn else [])
        seen_norm: set[str] = set()
        for line in pool_lines:
            nn = normalize_proxy_input(line)
            if not nn or nn in seen_norm:
                continue
            seen_norm.add(nn)
            label = _short_proxy_cell(line, 60)
            act = menu.addAction(label)
            act.setData(line)
        if menu.isEmpty():
            menu.addAction("(нет свободных строк в пуле)").setEnabled(False)
            menu.exec(btn.mapToGlobal(QPoint(0, btn.height())))
            return
        chosen = menu.exec(btn.mapToGlobal(QPoint(0, btn.height())))
        if not chosen:
            return
        new_line = chosen.data()
        if not isinstance(new_line, str):
            return
        new_norm = normalize_proxy_input(new_line)
        for r in range(self.bulk_preview_table.rowCount()):
            if r == row:
                continue
            ob = self.bulk_preview_table.cellWidget(r, 2)
            if isinstance(ob, QPushButton):
                opx = str(ob.property("proxy_full") or "")
                if normalize_proxy_input(opx) == new_norm:
                    self._bulk_set_proxy_cell(r, cur)
                    break
        self._bulk_set_proxy_cell(row, new_line)

    def _validate_bulk_rows(self) -> list[tuple[str, str]] | None:
        occ = self._occupied_norms_fn() if self._occupied_norms_fn else set()
        n = self.bulk_preview_table.rowCount()
        rows: list[tuple[str, str]] = []
        seen_norms: set[str] = set()
        for i in range(n):
            le = self.bulk_preview_table.cellWidget(i, 1)
            btn = self.bulk_preview_table.cellWidget(i, 2)
            nick = le.text().strip() if isinstance(le, QLineEdit) else ""
            px = str(btn.property("proxy_full") or "") if isinstance(btn, QPushButton) else ""
            px_n = normalize_proxy_input(px)
            if not nick:
                QMessageBox.warning(self, "Массовая регистрация", f"Пустой ник в строке {i + 1}.")
                return None
            if not px_n:
                QMessageBox.warning(
                    self,
                    "Массовая регистрация",
                    f"Не выбран прокси в строке {i + 1}. Нажми на поле прокси и выбери строку из пула.",
                )
                return None
            if px_n in seen_norms:
                QMessageBox.warning(self, "Массовая регистрация", "Один и тот же прокси указан для двух строк — нужны разные.")
                return None
            if px_n in occ:
                QMessageBox.warning(
                    self,
                    "Массовая регистрация",
                    f"Прокси в строке {i + 1} уже занят другим аккаунтом.",
                )
                return None
            seen_norms.add(px_n)
            rows.append((nick, px.strip()))
        return rows

    def get_bulk_rows(self) -> list[tuple[str, str]] | None:
        return self._bulk_result

    def attach_pool_peek(self, peek_unused_fn: Callable[[], str | None]) -> None:
        def _fill() -> None:
            p = peek_unused_fn()
            if p:
                self.reg_proxy.setText(p)
            else:
                QMessageBox.warning(
                    self,
                    "Пул",
                    "Нет свободных прокси (все из пула уже закреплены за аккаунтами или список пуст). "
                    "Добавь строки на вкладке «Прокси-пул» либо освободи прокси.",
                )

        self.btn_from_pool.clicked.connect(_fill)

    def mode(self) -> str:
        i = self.tabs.currentIndex()
        if i == 0:
            return "manual"
        if i == 1:
            return "register"
        return "bulk"

    def bulk_options(self) -> tuple[str, bool]:
        return self.bulk_class.currentText(), self.bulk_autostart.isChecked()


class ChangeProxyDialog(QDialog):
    def __init__(
        self,
        current_proxy: str | None,
        selectable_pool_proxies: list[str],
        parent=None,
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle("Сменить прокси аккаунта")
        self.resize(520, 320)
        self._pool = [p for p in selectable_pool_proxies if p]
        self._current = normalize_proxy_input(current_proxy)

        root = QVBoxLayout(self)
        root.setContentsMargins(16, 16, 16, 16)
        root.setSpacing(12)

        info = QLabel(
            "Режим «Свой прокси» — только поле ввода. "
            "Режим «Из пула» — выбор случайного или конкретной строки из доступных (свободные + текущий прокси этого аккаунта)."
        )
        info.setWordWrap(True)
        info.setStyleSheet("color: #a8a8a8; font-size: 12px;")
        root.addWidget(info)

        src_row = QHBoxLayout()
        self.rb_manual = QRadioButton("Вставить свой прокси")
        self.rb_pool = QRadioButton("Выбрать из прокси-пула")
        self.rb_manual.setChecked(True)
        self._src_group = QButtonGroup(self)
        self._src_group.addButton(self.rb_manual)
        self._src_group.addButton(self.rb_pool)
        self._src_group.setExclusive(True)
        src_row.addWidget(self.rb_manual)
        src_row.addWidget(self.rb_pool)
        src_row.addStretch(1)
        root.addLayout(src_row)

        self.manual_proxy = QLineEdit()
        self.manual_proxy.setPlaceholderText("host:port:user:pass")
        if self._current:
            self.manual_proxy.setText(self._current)
        root.addWidget(self.manual_proxy)

        self.pool_box = QGroupBox("Из прокси-пула")
        pv = QVBoxLayout(self.pool_box)
        self.rb_pool_random = QRadioButton("Случайный свободный")
        self.rb_pool_specific = QRadioButton("Конкретный из списка")
        self.rb_pool_random.setChecked(True)
        self._pool_pick_group = QButtonGroup(self)
        self._pool_pick_group.addButton(self.rb_pool_random)
        self._pool_pick_group.addButton(self.rb_pool_specific)
        self._pool_pick_group.setExclusive(True)
        pv.addWidget(self.rb_pool_random)
        pv.addWidget(self.rb_pool_specific)
        self.pool_combo = QComboBox()
        self.pool_combo.addItems(self._pool)
        self.pool_combo.setMaxVisibleItems(16)
        pv.addWidget(self.pool_combo)
        self.lbl_pool_hint = QLabel()
        self.lbl_pool_hint.setWordWrap(True)
        self.lbl_pool_hint.setStyleSheet("color: #8e8e8e; font-size: 11px;")
        pv.addWidget(self.lbl_pool_hint)

        root.addWidget(self.pool_box)

        row = QHBoxLayout()
        row.addStretch(1)
        self.btn_ok = QPushButton("Применить")
        self.btn_ok.setObjectName("primary")
        self.btn_cancel = QPushButton("Отмена")
        self.btn_cancel.setObjectName("secondary")
        self.btn_ok.clicked.connect(self._accept_if_valid)
        self.btn_cancel.clicked.connect(self.reject)
        row.addWidget(self.btn_ok)
        row.addWidget(self.btn_cancel)
        root.addLayout(row)

        self.rb_manual.toggled.connect(self._sync_mode)
        self.rb_pool.toggled.connect(self._sync_mode)
        self.rb_pool_random.toggled.connect(self._sync_mode)
        self.rb_pool_specific.toggled.connect(self._sync_mode)
        self._sync_mode()
        _hand_cursor_widgets(self)

    def _sync_mode(self) -> None:
        manual = self.rb_manual.isChecked()
        self.manual_proxy.setVisible(manual)
        self.pool_box.setVisible(not manual)
        if manual:
            return
        specific = self.rb_pool_specific.isChecked()
        self.pool_combo.setVisible(specific)
        self._refresh_pool_hint()

    def _refresh_pool_hint(self) -> None:
        if self.rb_manual.isChecked():
            return
        n = len(self._pool)
        if self.rb_pool_random.isChecked():
            others = [p for p in self._pool if normalize_proxy_input(p) != self._current]
            if n == 0:
                self.lbl_pool_hint.setText("В пуле нет доступных строк для этого аккаунта — добавь прокси или введи свой.")
            elif len(others) == 0:
                self.lbl_pool_hint.setText(
                    "Других свободных прокси нет — случайный выбор вернёт единственный доступный вариант (текущий или единственная строка в списке)."
                )
            else:
                self.lbl_pool_hint.setText(
                    f"Будет выбран один из {len(others)} прокси (случайно; текущий прокси не берём, пока есть альтернативы). Всего в списке: {n}."
                )
        else:
            self.lbl_pool_hint.setText(f"Выбери строку из списка ({n} вариантов). Полный прокси виден в выпадающем списке.")

    def chosen_proxy(self) -> str | None:
        if self.rb_manual.isChecked():
            return normalize_proxy_input(self.manual_proxy.text())
        if not self._pool:
            return None
        if self.rb_pool_random.isChecked():
            others = [p for p in self._pool if normalize_proxy_input(p) != self._current]
            pick_from = others if others else self._pool
            return normalize_proxy_input(random.choice(pick_from))
        return normalize_proxy_input(self.pool_combo.currentText())

    def _accept_if_valid(self) -> None:
        if self.rb_manual.isChecked():
            px = normalize_proxy_input(self.manual_proxy.text())
            if not px:
                QMessageBox.warning(self, "Прокси", "Введи прокси в формате host:port:user:pass.")
                return
            self.accept()
            return
        if not self._pool:
            QMessageBox.warning(
                self,
                "Прокси",
                "Нет доступных строк из пула для этого аккаунта. Добавь прокси в «Прокси-пул» или введи свой.",
            )
            return
        if self.rb_pool_specific.isChecked():
            px = normalize_proxy_input(self.pool_combo.currentText())
            if not px:
                QMessageBox.warning(self, "Прокси", "Выбери прокси из списка.")
                return
        self.accept()


class ExportAccountsDialog(QDialog):
    """Экспорт в .txt: пресеты с подсветкой выбора; вставка полей ставит «:» только между плейсхолдерами."""

    COLON_FIELD_META: list[tuple[str, str]] = [
        ("wallet_address", "Адрес кошелька (Sol)"),
        ("private_key", "Приватный ключ"),
        ("proxy", "Прокси"),
        ("nickname", "Никнейм"),
        ("balance_str", "Баланс $SIEGE"),
        ("level", "Уровень"),
        ("wave", "Волна"),
        ("account_id", "ID аккаунта"),
        ("total_earned_str", "Всего $SIEGE"),
    ]

    PRESET_LINE: dict[str, str] = {
        "colon_addr_key_proxy": "{wallet_address}:{private_key}:{proxy}",
        "colon_key_proxy": "{private_key}:{proxy}",
        "colon_full_stats": "{wallet_address}:{nickname}:{balance_str}:{level}:{wave}:{proxy}",
    }

    def __init__(self, rows: list[dict[str, Any]], parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Экспорт аккаунтов в файл")
        self.resize(720, 620)
        self._rows = rows
        self._mode = "template"
        self._insert_btns: list[QPushButton] = []
        self._preset_buttons: dict[str, QPushButton] = {}
        self._suppress_line_changed = False

        root = QVBoxLayout(self)
        root.setContentsMargins(18, 18, 18, 18)
        root.setSpacing(14)

        intro = QLabel(
            "Одна строка в файле = один аккаунт. В блоке «Свой шаблон» кнопки полей вставляют плейсхолдер; "
            "<b>двоеточие</b> добавляется только <b>между</b> соседними плейсхолдерами (после последнего — нет). "
            "Лишние двоеточия в конце строки при сохранении убираются."
        )
        intro.setWordWrap(True)
        intro.setStyleSheet("color: #c8c8c8; font-size: 12px;")
        root.addWidget(intro)

        self.lbl_choice = QLabel("Выбрано: свой шаблон")
        self.lbl_choice.setStyleSheet("color: #7ce0a8; font-weight: 700; font-size: 12px;")
        root.addWidget(self.lbl_choice)

        quick_box = QGroupBox("Быстрые пресеты")
        quick_box.setStyleSheet("QGroupBox { color: #e0e0e0; font-weight: 700; }")
        qg = QGridLayout(quick_box)
        qg.setSpacing(10)
        self._preset_group = QButtonGroup(self)
        self._preset_group.setExclusive(True)
        presets: list[tuple[str, str, str]] = [
            ("Только приватные ключи (строка = ключ)", "keys_only", "keys_only"),
            ("Адрес:ключ:прокси", "colon_addr_key_proxy", "template"),
            ("Ключ:прокси", "colon_key_proxy", "template"),
            ("Адрес:ник:баланс:ур:волна:прокси", "colon_full_stats", "template"),
        ]
        for i, (caption, pid, mode) in enumerate(presets):
            b = QPushButton(caption)
            b.setCheckable(True)
            b.setObjectName("exportPresetBtn")
            b.clicked.connect(partial(self._apply_quick, pid, mode, caption))
            self._preset_group.addButton(b)
            self._preset_buttons[pid] = b
            qg.addWidget(b, i // 2, i % 2)
        root.addWidget(quick_box)

        fmt_box = QGroupBox("Свой шаблон")
        fmt_box.setStyleSheet("QGroupBox { color: #e0e0e0; font-weight: 700; }")
        fv = QVBoxLayout(fmt_box)
        tmpl_hint = QLabel(
            "Плейсхолдеры: <code>{private_key}</code> <code>{wallet_address}</code> <code>{proxy}</code> "
            "<code>{nickname}</code> <code>{account_id}</code> <code>{balance_str}</code> "
            "<code>{level}</code> <code>{wave}</code> <code>{total_earned_str}</code>"
        )
        tmpl_hint.setWordWrap(True)
        tmpl_hint.setStyleSheet("color: #9a9a9a; font-size: 11px;")
        fv.addWidget(tmpl_hint)

        ins_lbl = QLabel("Вставка в позицию курсора")
        ins_lbl.setStyleSheet("color: #b0b0b0; font-size: 11px; margin-top: 2px;")
        fv.addWidget(ins_lbl)
        ins_grid = QGridLayout()
        ins_grid.setSpacing(6)
        ncols = 4
        for i, (key, title) in enumerate(self.COLON_FIELD_META):
            short = title.split("(")[0].strip()
            if len(short) > 16:
                short = short[:15] + "…"
            b = QPushButton(short)
            b.setObjectName("secondary")
            b.setToolTip(f"Вставить {{{key}}} у курсора")
            b.clicked.connect(partial(self._insert_field, key))
            self._insert_btns.append(b)
            ins_grid.addWidget(b, i // ncols, i % ncols)
        fv.addLayout(ins_grid)

        self.line_format = QLineEdit()
        self.line_format.setPlaceholderText("{wallet_address}:{private_key}:{proxy}")
        self.line_format.textChanged.connect(self._on_line_format_edited)
        fv.addWidget(self.line_format)
        root.addWidget(fmt_box)

        row_btn = QHBoxLayout()
        self.btn_ok = QPushButton("Сохранить в файл…")
        self.btn_ok.setObjectName("primary")
        self.btn_cancel = QPushButton("Отмена")
        self.btn_cancel.setObjectName("secondary")
        self.btn_cancel.clicked.connect(self.reject)
        self.btn_ok.clicked.connect(self._do_export)
        row_btn.addStretch(1)
        row_btn.addWidget(self.btn_ok)
        row_btn.addWidget(self.btn_cancel)
        root.addLayout(row_btn)
        _hand_cursor_widgets(self)

        self.line_format.clear()
        self._set_mode_ui()

    def _uncheck_export_presets(self) -> None:
        for btn in self._preset_group.buttons():
            btn.blockSignals(True)
            btn.setChecked(False)
            btn.blockSignals(False)

    def _mark_custom_template(self) -> None:
        if self._mode != "template":
            return
        self._uncheck_export_presets()
        self.lbl_choice.setText("Выбрано: свой шаблон")

    def _on_line_format_edited(self, *_args: object) -> None:
        if self._suppress_line_changed:
            return
        if self._mode != "template":
            return
        self._mark_custom_template()

    def _set_mode_ui(self) -> None:
        template = self._mode == "template"
        self.line_format.setEnabled(template)
        for b in self._insert_btns:
            b.setEnabled(template)

    def _apply_quick(self, preset_id: str, mode: str, caption: str) -> None:
        self._mode = mode
        self.lbl_choice.setText(f"Выбрано: {caption}")
        self._suppress_line_changed = True
        try:
            if mode == "template":
                line = self.PRESET_LINE.get(preset_id, "")
                if line:
                    self.line_format.setText(line)
            else:
                self.line_format.clear()
        finally:
            self._suppress_line_changed = False
        self._set_mode_ui()

    @staticmethod
    def _normalize_template_line(s: str) -> str:
        t = s.strip()
        while t.endswith(":"):
            t = t[:-1]
        return t

    @staticmethod
    def _colon_between_placeholders_needed(left: str) -> bool:
        """Нужно ли поставить ':' перед новым {…}, если слева от курсора уже закончился плейсхолдер."""
        if not left:
            return False
        s = left.rstrip()
        if not s.endswith("}"):
            return False
        j = s.rfind("}")
        tail = left[j + 1 :]
        t = tail.lstrip()
        if not t:
            return True
        if t.startswith(":"):
            return False
        if t.startswith("{"):
            return True
        return False

    def _insert_field(self, field_key: str) -> None:
        le = self.line_format
        pos = le.cursorPosition()
        text = le.text()
        left = text[:pos]
        sep = ":" if self._colon_between_placeholders_needed(left) else ""
        ins = sep + "{" + field_key + "}"
        le.setText(left + ins + text[pos:])
        le.setCursorPosition(pos + len(ins))
        le.setFocus()

    def _row_val(self, rw: dict[str, Any], key: str) -> str:
        v = rw.get(key)
        if v is None:
            return ""
        return str(v).strip()

    def _compose_text(self) -> tuple[str, bool]:
        lines: list[str] = []
        needs_private = False

        if self._mode == "keys_only":
            needs_private = True
            for rw in self._rows:
                pk = (rw.get("private_key") or "").strip()
                if pk:
                    lines.append(pk)
            return "\n".join(lines) + ("\n" if lines else ""), needs_private

        tpl = self._normalize_template_line(self.line_format.text())
        if not tpl:
            raise ValueError("Введи шаблон строки в поле или выбери «только ключи».")
        needs_private = "{private_key}" in tpl
        for rw in self._rows:
            lines.append(_export_template_format(tpl, rw))
        return "\n".join(lines) + ("\n" if lines else ""), needs_private

    def _do_export(self) -> None:
        if self._mode == "template":
            if not self._normalize_template_line(self.line_format.text()):
                QMessageBox.warning(
                    self,
                    "Формат",
                    "Введи шаблон в поле или выбери пресет «только ключи».",
                )
                return
        try:
            text, need_sec = self._compose_text()
        except ValueError as e:
            QMessageBox.warning(self, "Экспорт", str(e))
            return
        if not text.strip():
            QMessageBox.warning(self, "Экспорт", "Нечего записать — нет данных по аккаунтам.")
            return
        if need_sec:
            if (
                QMessageBox.question(
                    self,
                    "Конфиденциально",
                    "В файл попадут приватные ключи открытым текстом. "
                    "Храни только у себя и не отправляй никому.",
                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                    QMessageBox.StandardButton.No,
                )
                != QMessageBox.StandardButton.Yes
            ):
                return
        path, _ = QFileDialog.getSaveFileName(
            self,
            "Сохранить экспорт",
            "",
            "Текст (*.txt);;Все файлы (*)",
        )
        if not path:
            return
        Path(path).write_text(text, encoding="utf-8")
        QMessageBox.information(self, "Готово", f"Строк в файле: {len(text.splitlines())}.")
        self.accept()


class MainWindow(QMainWindow):
    _sig_detail_poll = Signal(dict)
    _sig_detail_manual = Signal(dict)
    ACCT_TABLE_ROW_HEIGHT = 48
    ACCT_CARD_WIDTH_MIN = 300
    ACCT_CARD_WIDTH_MAX = 540

    def __init__(self, root: Path) -> None:
        super().__init__()
        self.root = root
        self.account_store = AccountStore(root)
        self.wallet_store = WalletStore(root)
        self.state_store = StateStore(root)
        self.proxy_pool = ProxyPoolStore(root)
        self.app_settings_store = AppSettingsStore(root)
        self.app_settings = self.app_settings_store.load()
        self.account_cache_store = AccountManagerCacheStore(root)

        self.bridge = WorkerBridge()
        self.bridge.event_signal.connect(self._on_worker_event)
        self.bridge.log_signal.connect(self._on_worker_log)
        self.workers: dict[str, BotWorker] = {}
        self.snapshots: dict[str, BotSnapshot] = {}
        self.cards: dict[str, AccountCard] = {}
        self.details_cache: dict[str, dict] = {}
        self._accounts_list: list[AccountConfig] = []
        self._details_poll_busy = False
        self._fleet_expanded = False
        self.FLEET_GRID_COLS_EXPANDED = 2

        self._sig_detail_poll.connect(self._on_detail_poll_done)
        self._sig_detail_manual.connect(self._on_detail_manual_done)

        self._ui_refresh_timer = QTimer(self)
        self._ui_refresh_timer.setSingleShot(True)
        self._ui_refresh_timer.setInterval(120)
        self._ui_refresh_timer.timeout.connect(self._refresh_cards)

        self.setWindowTitle("SolSiege Ферма")
        self.resize(1320, 860)

        self._build_ui()
        self._load_accounts()
        self.timer = QTimer(self)
        self.timer.timeout.connect(self._periodic_light_refresh)
        self.timer.start(10000)
        self.poll_timer = QTimer(self)
        self.poll_timer.timeout.connect(self._refresh_running_details)
        self.poll_timer.start(12000)

    def _build_ui(self) -> None:
        central = QWidget()
        central.setObjectName("centralRoot")
        self.setCentralWidget(central)
        outer = QHBoxLayout(central)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(0)

        sidebar = QFrame()
        sidebar.setObjectName("sidebar")
        sidebar.setFixedWidth(256)
        sv = QVBoxLayout(sidebar)
        sv.setContentsMargins(16, 24, 16, 24)
        hdr = QLabel("SolSiege Ферма")
        hdr.setStyleSheet("font-size: 18px; font-weight: 900; color: #ffffff; padding: 0 8px;")
        sv.addWidget(hdr)
        sv.addSpacing(20)

        self._nav_group = QButtonGroup(self)
        self.btn_nav_dash = QPushButton("  ◉  Дашборд")
        self.btn_nav_proxy = QPushButton("  ◎  Прокси-пул")
        self.btn_nav_accounts = QPushButton("  ▤  Аккаунты")
        self.btn_nav_settings = QPushButton("  ⚙  Настройки")
        for b in (self.btn_nav_dash, self.btn_nav_proxy, self.btn_nav_accounts, self.btn_nav_settings):
            b.setObjectName("navBtn")
            b.setCheckable(True)
            self._nav_group.addButton(b)
            sv.addWidget(b)
        self.btn_nav_dash.setChecked(True)

        sv.addStretch(1)
        self.btn_deploy = QPushButton("  🚀  Добавить аккаунт")
        self.btn_deploy.setObjectName("deployBtn")
        deploy_row = QHBoxLayout()
        deploy_row.addStretch(1)
        deploy_row.addWidget(self.btn_deploy)
        deploy_row.addStretch(1)
        sv.addLayout(deploy_row)
        outer.addWidget(sidebar)

        body = QVBoxLayout()
        body.setContentsMargins(0, 0, 0, 0)
        body.setSpacing(0)

        self.stack = QStackedWidget()
        self._page_dashboard = QWidget()
        self._page_proxy = QWidget()
        self._page_accounts = QWidget()
        self._page_settings = QWidget()
        self.stack.addWidget(self._page_dashboard)
        self.stack.addWidget(self._page_proxy)
        self.stack.addWidget(self._page_accounts)
        self.stack.addWidget(self._page_settings)
        body.addWidget(self.stack, 1)
        outer.addLayout(body, 1)

        self._build_dashboard_page()
        self._build_proxy_page()
        self._build_accounts_manager_page()
        self._build_settings_page()

        self.btn_nav_dash.clicked.connect(lambda: self._goto_page(0))
        self.btn_nav_proxy.clicked.connect(lambda: self._goto_page(1))
        self.btn_nav_accounts.clicked.connect(lambda: self._goto_page(2))
        self.btn_nav_settings.clicked.connect(lambda: self._goto_page(3))
        self.btn_deploy.clicked.connect(self._add_account)
        _hand_cursor_widgets(self)

    def _schedule_ui_refresh(self) -> None:
        self._ui_refresh_timer.start()

    def _expand_fleet_view(self) -> None:
        if self._fleet_expanded:
            return
        self._fleet_expanded = True
        self._normal_fleet_layout.removeWidget(self.fleet_shell)
        self.fleet_shell.setParent(None)
        self._expanded_fleet_layout.addWidget(self.fleet_shell, 1)
        self._dash_top_block.setVisible(False)
        self._normal_mid.setVisible(False)
        self._expanded_mid.setVisible(True)
        self.btn_fleet_expand.setVisible(False)
        self._relayout_fleet_cards()

    def _collapse_fleet_view(self) -> None:
        if not self._fleet_expanded:
            return
        self._fleet_expanded = False
        self._expanded_fleet_layout.removeWidget(self.fleet_shell)
        self.fleet_shell.setParent(None)
        self._normal_fleet_layout.addWidget(self.fleet_shell, 1)
        self._dash_top_block.setVisible(True)
        self._normal_mid.setVisible(True)
        self._expanded_mid.setVisible(False)
        self.btn_fleet_expand.setVisible(True)
        self._relayout_fleet_cards()

    def _fleet_grid_column_count(self) -> int:
        return self.FLEET_GRID_COLS_EXPANDED if self._fleet_expanded else 1

    def _relayout_fleet_cards(self) -> None:
        if not hasattr(self, "cards_grid"):
            return
        cols = max(1, self._fleet_grid_column_count())
        while self.cards_grid.count():
            it = self.cards_grid.takeAt(0)
            w = it.widget()
            if w:
                w.setParent(None)
        for i, acc in enumerate(self._accounts_list):
            card = self.cards.get(acc.account_id)
            if not card:
                continue
            self.cards_grid.addWidget(card, i // cols, i % cols)
        n = len(self._accounts_list)
        br = (n + cols - 1) // cols if n else 0
        self.cards_grid.setRowStretch(br, 1)
        for c in range(cols):
            self.cards_grid.setColumnStretch(c, 1)
        self._sync_account_card_widths()

    def _sync_account_card_widths(self) -> None:
        if not hasattr(self, "scroll") or not hasattr(self, "cards_grid") or not self.cards:
            return
        vw = self.scroll.viewport().width()
        if vw < 120:
            return
        cols = max(1, self._fleet_grid_column_count())
        sp = self.cards_grid.horizontalSpacing()
        m = self.cards_grid.contentsMargins()
        inner = max(200, vw - m.left() - m.right())
        if cols > 1:
            cell = (inner - sp * (cols - 1)) // cols
        else:
            cell = inner
        card_w = max(self.ACCT_CARD_WIDTH_MIN, min(self.ACCT_CARD_WIDTH_MAX, cell - 12))
        for c in self.cards.values():
            c.setFixedWidth(card_w)

    def resizeEvent(self, event) -> None:
        super().resizeEvent(event)
        self._sync_account_card_widths()

    def eventFilter(self, obj: QObject, ev: QEvent) -> bool:
        if hasattr(self, "scroll") and obj is self.scroll.viewport() and ev.type() == QEvent.Type.Resize:
            self._sync_account_card_widths()
        return super().eventFilter(obj, ev)

    def _goto_page(self, idx: int) -> None:
        if idx != 0 and getattr(self, "_fleet_expanded", False):
            self._collapse_fleet_view()
        self.stack.setCurrentIndex(idx)
        mapping = {
            0: self.btn_nav_dash,
            1: self.btn_nav_proxy,
            2: self.btn_nav_accounts,
            3: self.btn_nav_settings,
        }
        if idx == 2:
            QTimer.singleShot(0, self._fill_accounts_manager_table)
        b = mapping.get(idx)
        if b:
            b.setChecked(True)

    def _build_dashboard_page(self) -> None:
        layout = QVBoxLayout(self._page_dashboard)
        layout.setContentsMargins(40, 32, 40, 36)
        layout.setSpacing(0)

        self._dash_top_block = QWidget()
        dtop = QVBoxLayout(self._dash_top_block)
        dtop.setContentsMargins(0, 0, 0, 0)
        dtop.setSpacing(0)

        head = QHBoxLayout()
        head.setSpacing(20)
        left_t = QVBoxLayout()
        left_t.setSpacing(6)
        t1 = QLabel("Обзор фермы")
        t1.setStyleSheet("font-size: 34px; font-weight: 900; color: #ffffff; letter-spacing: -0.02em;")
        t2 = QLabel("Метрики фермы и управление аккаунтами")
        t2.setStyleSheet("color: #8a8a8a; font-weight: 500; font-size: 14px;")
        left_t.addWidget(t1)
        left_t.addWidget(t2)
        head.addLayout(left_t)
        head.addStretch(1)
        self.btn_add_header = QPushButton("+ Аккаунт")
        self.btn_add_header.setObjectName("secondary")
        self.btn_resume_all = QPushButton("▶ Запустить всех")
        self.btn_resume_all.setObjectName("primary")
        self.btn_stop_all = QPushButton("■ Остановить всех")
        self.btn_stop_all.setObjectName("stopAllBtn")
        head.addWidget(self.btn_add_header)
        head.addWidget(self.btn_resume_all)
        head.addWidget(self.btn_stop_all)
        dtop.addLayout(head)
        dtop.addSpacing(28)

        self.metrics_row = QHBoxLayout()
        self.metrics_row.setSpacing(16)
        self.metric_active, self.lbl_m_active = _metric_card("⬡", "0", "Ботов онлайн", "#3fff8b", "Live")
        self.metric_proxy, self.lbl_m_proxy = _metric_card("⬢", "0", "Свободных прокси", "#6e9bff", "Farm")
        self.metric_ch, self.lbl_m_ch = _metric_card(
            "◎",
            "0",
            "Всего $SIEGE на ферме",
            "#7ae6ff",
            "$SIEGE",
            value_fixed_width=292,
            value_monospace=True,
        )
        self.metric_faults, self.lbl_m_faults = _metric_card("!", "0", "Ошибки", "#ff716c", "Alert")
        for w in (self.metric_active, self.metric_proxy, self.metric_ch, self.metric_faults):
            self.metrics_row.addWidget(w, 1)
        dtop.addLayout(self.metrics_row)
        dtop.addSpacing(28)
        layout.addWidget(self._dash_top_block, 0)

        self.fleet_shell = QFrame()
        self.fleet_shell.setObjectName("fleetShell")
        fsl = QVBoxLayout(self.fleet_shell)
        fsl.setContentsMargins(0, 0, 0, 0)
        fsl.setSpacing(0)

        strip = QFrame()
        strip.setObjectName("fleetStrip")
        strip_l = QHBoxLayout(strip)
        strip_l.setContentsMargins(26, 22, 26, 18)
        strip_head = QVBoxLayout()
        strip_head.setSpacing(4)
        fleet_title = QLabel("Активность фермы")
        fleet_title.setStyleSheet("font-size: 19px; font-weight: 800; color: #f4f4f4; letter-spacing: -0.02em;")
        self.lbl_fleet_meta = QLabel("—")
        self.lbl_fleet_meta.setStyleSheet("font-size: 12px; color: #6f6f6f; font-weight: 500;")
        strip_head.addWidget(fleet_title)
        strip_head.addWidget(self.lbl_fleet_meta)
        strip_l.addLayout(strip_head, 1)
        self.btn_fleet_expand = QPushButton("Развернуть")
        self.btn_fleet_expand.setObjectName("secondary")
        self.btn_fleet_expand.setToolTip("Показать все аккаунты на ширину окна сеткой (как панель мониторинга)")
        strip_l.addWidget(self.btn_fleet_expand, 0, Qt.AlignmentFlag.AlignBottom)
        live = QLabel("● ONLINE")
        live.setStyleSheet(
            "color: #3fff8b; font-size: 10px; font-weight: 800; letter-spacing: 0.14em; "
            "padding: 8px 12px; background-color: rgba(63, 255, 139, 0.08); border-radius: 10px; "
            "border: 1px solid #2a4d38;"
        )
        strip_l.addWidget(live, 0, Qt.AlignmentFlag.AlignBottom)
        fsl.addWidget(strip)

        scroll_area_wrap = QWidget()
        saw = QVBoxLayout(scroll_area_wrap)
        saw.setContentsMargins(14, 6, 14, 18)
        saw.setSpacing(0)
        self.scroll = QScrollArea()
        self.scroll.setObjectName("fleetScroll")
        self.scroll.setWidgetResizable(True)
        self.scroll.setFrameShape(QFrame.Shape.NoFrame)
        self.scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.cards_container = QWidget()
        self.cards_container.setObjectName("fleetInner")
        self.cards_grid = QGridLayout(self.cards_container)
        self.cards_grid.setSpacing(16)
        self.cards_grid.setContentsMargins(4, 6, 6, 4)
        self.cards_grid.setHorizontalSpacing(16)
        self.scroll.setWidget(self.cards_container)
        saw.addWidget(self.scroll)
        self.scroll.viewport().installEventFilter(self)
        fsl.addWidget(scroll_area_wrap, 1)

        self._normal_fleet_holder = QWidget()
        self._normal_fleet_layout = QVBoxLayout(self._normal_fleet_holder)
        self._normal_fleet_layout.setContentsMargins(0, 0, 0, 0)
        self._normal_fleet_layout.addWidget(self.fleet_shell, 1)

        mid = QHBoxLayout()
        mid.setSpacing(20)
        left_col = QVBoxLayout()
        left_col.setSpacing(0)
        left_col.addWidget(self._normal_fleet_holder, 1)
        mid.addLayout(left_col, 5)

        self._dash_right_panel = QWidget()
        right_outer = QVBoxLayout(self._dash_right_panel)
        right_outer.setContentsMargins(0, 0, 0, 0)
        right_col = QVBoxLayout()
        right_col.setSpacing(16)
        node = QFrame()
        node.setObjectName("surfacePanel")
        node_l = QVBoxLayout(node)
        node_l.setContentsMargins(22, 22, 22, 20)
        node_l.setSpacing(12)
        node_hdr = QLabel("Сводка фермы")
        node_hdr.setObjectName("sectionTitle")
        node_l.addWidget(node_hdr)
        self.lbl_node_na = QLabel("Прокси в пуле: —")
        self.lbl_node_eu = QLabel("Аккаунтов: —")
        self.lbl_node_ap = QLabel("Онлайн: —")
        for x in (self.lbl_node_na, self.lbl_node_eu, self.lbl_node_ap):
            x.setStyleSheet("color: #a0a0a0; font-size: 13px;")
        node_l.addWidget(self.lbl_node_na)
        node_l.addWidget(self.lbl_node_eu)
        node_l.addWidget(self.lbl_node_ap)
        self.btn_refresh_pool_dash = QPushButton("Обновить сводку")
        self.btn_refresh_pool_dash.setObjectName("secondary")
        node_l.addWidget(self.btn_refresh_pool_dash)
        right_col.addWidget(node)

        self.log_box = QPlainTextEdit()
        self.log_box.setObjectName("eventLog")
        self.log_box.setReadOnly(True)
        log_hdr = QLabel("Лог событий")
        log_hdr.setObjectName("sectionTitle")
        right_col.addWidget(log_hdr)
        right_col.addWidget(self.log_box, 1)
        right_outer.addLayout(right_col)
        mid.addWidget(self._dash_right_panel, 4)

        self._normal_mid = QWidget()
        nm_lay = QHBoxLayout(self._normal_mid)
        nm_lay.setContentsMargins(0, 0, 0, 0)
        nm_lay.addLayout(mid)

        self._expanded_fleet_holder = QWidget()
        self._expanded_fleet_layout = QVBoxLayout(self._expanded_fleet_holder)
        self._expanded_fleet_layout.setContentsMargins(0, 0, 0, 0)

        self._expanded_mid = QWidget()
        exp_lay = QVBoxLayout(self._expanded_mid)
        exp_lay.setContentsMargins(0, 0, 0, 0)
        exp_tool = QHBoxLayout()
        self.btn_fleet_expand_close = QPushButton("✕")
        self.btn_fleet_expand_close.setObjectName("secondary")
        self.btn_fleet_expand_close.setFixedSize(44, 36)
        self.btn_fleet_expand_close.setToolTip("Вернуться к обычному дашборду")
        exp_hdr = QLabel("Активность фермы · все аккаунты")
        exp_hdr.setStyleSheet("font-size: 15px; font-weight: 800; color: #eaeaea;")
        self.btn_resume_fleet_exp = QPushButton("▶ Запустить всех")
        self.btn_resume_fleet_exp.setObjectName("primary")
        self.btn_stop_fleet_exp = QPushButton("■ Остановить всех")
        self.btn_stop_fleet_exp.setObjectName("stopAllBtn")
        exp_tool.addWidget(self.btn_fleet_expand_close)
        exp_tool.addWidget(exp_hdr)
        exp_tool.addStretch(1)
        exp_tool.addWidget(self.btn_resume_fleet_exp)
        exp_tool.addWidget(self.btn_stop_fleet_exp)
        exp_lay.addLayout(exp_tool)
        exp_lay.addWidget(self._expanded_fleet_holder, 1)
        self._expanded_mid.hide()

        layout.addWidget(self._normal_mid, 1)
        layout.addWidget(self._expanded_mid, 1)

        self.btn_add_header.clicked.connect(self._add_account)
        self.btn_resume_all.clicked.connect(self._resume_all)
        self.btn_stop_all.clicked.connect(self._stop_all)
        self.btn_refresh_pool_dash.clicked.connect(self._refresh_proxy_sidebar_counts)
        self.btn_fleet_expand.clicked.connect(self._expand_fleet_view)
        self.btn_fleet_expand_close.clicked.connect(self._collapse_fleet_view)
        self.btn_resume_fleet_exp.clicked.connect(self._resume_all)
        self.btn_stop_fleet_exp.clicked.connect(self._stop_all)

    def _build_proxy_page(self) -> None:
        layout = QVBoxLayout(self._page_proxy)
        layout.setContentsMargins(32, 28, 32, 28)
        layout.setSpacing(0)

        shell = QFrame()
        shell.setObjectName("fleetShell")
        sl = QVBoxLayout(shell)
        sl.setContentsMargins(0, 0, 0, 0)
        sl.setSpacing(0)

        strip = QFrame()
        strip.setObjectName("fleetStrip")
        sr = QHBoxLayout(strip)
        sr.setContentsMargins(26, 22, 26, 18)
        head = QVBoxLayout()
        head.setSpacing(4)
        t = QLabel("Прокси-пул")
        t.setStyleSheet("font-size: 19px; font-weight: 800; color: #f4f4f4; letter-spacing: -0.02em;")
        sub = QLabel(
            "До сотен строк: host:port:user:pass — по одной на строку. "
            "Прокси из новых аккаунтов подставляются в список автоматически; для строки видно, какой аккаунт закреплён."
        )
        sub.setStyleSheet("font-size: 12px; color: #6f6f6f;")
        sub.setWordWrap(True)
        head.addWidget(t)
        head.addWidget(sub)
        sr.addLayout(head, 1)
        pool_badge = QLabel("POOL")
        pool_badge.setStyleSheet(
            "color: #6e9bff; font-size: 10px; font-weight: 800; letter-spacing: 0.14em; "
            "padding: 8px 12px; background-color: rgba(110, 155, 255, 0.1); border-radius: 10px; "
            "border: 1px solid #2a3d5c;"
        )
        sr.addWidget(pool_badge, 0, Qt.AlignmentFlag.AlignBottom)
        sl.addWidget(strip)

        editor_block = QWidget()
        eb = QVBoxLayout(editor_block)
        eb.setContentsMargins(18, 16, 18, 12)
        eb.setSpacing(12)
        self.proxy_bulk_input = QPlainTextEdit()
        self.proxy_bulk_input.setPlaceholderText("82.22.210.160:8002:user:pass\n...")
        self.proxy_bulk_input.setMinimumHeight(160)
        eb.addWidget(self.proxy_bulk_input)
        row = QHBoxLayout()
        row.setSpacing(10)
        self.btn_proxy_add_bulk = QPushButton("Добавить в пул")
        self.btn_proxy_add_bulk.setObjectName("primary")
        self.btn_proxy_clear_input = QPushButton("Очистить поле")
        self.btn_proxy_clear_input.setObjectName("secondary")
        row.addWidget(self.btn_proxy_add_bulk)
        row.addWidget(self.btn_proxy_clear_input)
        row.addStretch(1)
        self.lbl_proxy_stats = QLabel("В пуле: 0")
        self.lbl_proxy_stats.setStyleSheet("font-size: 12px; color: #8a8a8a; font-weight: 600;")
        row.addWidget(self.lbl_proxy_stats)
        eb.addLayout(row)
        sl.addWidget(editor_block)

        list_block = QWidget()
        lb_l = QVBoxLayout(list_block)
        lb_l.setContentsMargins(14, 4, 14, 18)
        lb_l.setSpacing(10)
        self.proxy_list = QListWidget()
        self.proxy_list.setSelectionMode(self.proxy_list.SelectionMode.ExtendedSelection)
        lb_l.addWidget(self.proxy_list, 1)
        row2 = QHBoxLayout()
        row2.setSpacing(10)
        self.btn_proxy_del = QPushButton("Удалить выбранные")
        self.btn_proxy_del.setObjectName("secondary")
        self.btn_proxy_clear_all = QPushButton("Убрать свободные")
        self.btn_proxy_clear_all.setObjectName("secondary")
        row2.addWidget(self.btn_proxy_del)
        row2.addWidget(self.btn_proxy_clear_all)
        row2.addStretch(1)
        lb_l.addLayout(row2)
        sl.addWidget(list_block, 1)

        layout.addWidget(shell, 1)

        self.btn_proxy_add_bulk.clicked.connect(self._proxy_add_bulk)
        self.btn_proxy_clear_input.clicked.connect(self.proxy_bulk_input.clear)
        self.btn_proxy_del.clicked.connect(self._proxy_delete_selected)
        self.btn_proxy_clear_all.clicked.connect(self._proxy_clear_all)
        self._reload_proxy_list()

    def _build_accounts_manager_page(self) -> None:
        layout = QVBoxLayout(self._page_accounts)
        layout.setContentsMargins(32, 28, 32, 28)
        layout.setSpacing(16)
        head = QHBoxLayout()
        head.setSpacing(16)
        tit = QVBoxLayout()
        t = QLabel("Менеджер аккаунтов")
        t.setStyleSheet("font-size: 22px; font-weight: 900; color: #ffffff; letter-spacing: -0.02em;")
        sub = QLabel(
            "Сводка по всем аккаунтам: баланс $SIEGE, уровень и волна подтягиваются с дашборда; после перезапуска "
            "показываются последние сохранённые значения."
        )
        sub.setWordWrap(True)
        sub.setStyleSheet("font-size: 12px; color: #8a8a8a; max-width: 720px;")
        tit.addWidget(t)
        tit.addWidget(sub)
        head.addLayout(tit, 1)
        self.btn_acct_refresh = QPushButton("Обновить таблицу")
        self.btn_acct_refresh.setObjectName("secondary")
        self.btn_acct_delete_banned = QPushButton("Удалить забаненные")
        self.btn_acct_delete_banned.setObjectName("danger")
        self.btn_acct_delete_banned.setToolTip("Удалить все аккаунты с флагом БАН (ACCOUNT_BANNED) из базы, кошельков и состояния")
        self.btn_acct_export = QPushButton("Экспорт в .txt…")
        self.btn_acct_export.setObjectName("primary")
        head.addWidget(self.btn_acct_refresh, 0, Qt.AlignmentFlag.AlignBottom)
        head.addWidget(self.btn_acct_delete_banned, 0, Qt.AlignmentFlag.AlignBottom)
        head.addWidget(self.btn_acct_export, 0, Qt.AlignmentFlag.AlignBottom)
        layout.addLayout(head)
        self.acct_table = QTableWidget(0, 9)
        self.acct_table.setHorizontalHeaderLabels(
            ["№", "Никнейм", "ID аккаунта", "Кошелёк Sol", "$SIEGE", "Ур.", "Волна", "Онлайн", "Прокси"]
        )
        vh = self.acct_table.verticalHeader()
        vh.setVisible(False)
        vh.setDefaultSectionSize(self.ACCT_TABLE_ROW_HEIGHT)
        vh.setSectionResizeMode(QHeaderView.ResizeMode.Fixed)
        acct_hdr = self.acct_table.horizontalHeader()
        acct_hdr.setStretchLastSection(False)
        for c in (0, 3, 4, 5, 6, 7):
            acct_hdr.setSectionResizeMode(c, QHeaderView.ResizeMode.ResizeToContents)
        acct_hdr.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        acct_hdr.setSectionResizeMode(2, QHeaderView.ResizeMode.Fixed)
        self.acct_table.setColumnWidth(2, 108)
        acct_hdr.setSectionResizeMode(8, QHeaderView.ResizeMode.Fixed)
        self.acct_table.setColumnWidth(8, 340)
        self.acct_table.setWordWrap(False)
        # Цвет строк задаём в коде (в т.ч. бан), иначе QSS и alternate-background перекрывают setBackground.
        self.acct_table.setAlternatingRowColors(False)
        self.acct_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.acct_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.acct_table.setShowGrid(True)
        layout.addWidget(self.acct_table, 1)
        self.btn_acct_refresh.clicked.connect(self._fill_accounts_manager_table)
        self.btn_acct_delete_banned.clicked.connect(self._delete_banned_accounts_clicked)
        self.btn_acct_export.clicked.connect(self._export_accounts_clicked)
        _hand_cursor_widgets(self._page_accounts)

    def _build_settings_page(self) -> None:
        layout = QVBoxLayout(self._page_settings)
        layout.setContentsMargins(32, 28, 32, 28)
        layout.addWidget(QLabel("Настройки"))
        f = QFormLayout()
        self.set_sleep_min = QSpinBox()
        self.set_sleep_min.setRange(8, 120)
        self.set_sleep_min.setValue(max(8, int(self.app_settings.sleep_min_seconds)))
        self.set_sleep_max = QSpinBox()
        self.set_sleep_max.setRange(8, 300)
        smax = int(self.app_settings.sleep_max_seconds)
        self.set_sleep_max.setMinimum(self.set_sleep_min.value())
        self.set_sleep_max.setValue(max(self.set_sleep_min.value(), smax))
        self.set_sleep_min.valueChanged.connect(self._on_settings_sleep_min_changed)
        self.chk_chest = QCheckBox("Авто-открытие сундуков")
        self.chk_chest.setChecked(self.app_settings.auto_open_chests)
        self.chk_equip = QCheckBox("Авто-экипировка")
        self.chk_equip.setChecked(self.app_settings.auto_equip)
        self.chk_stats = QCheckBox("Авто-статы")
        self.chk_stats.setChecked(self.app_settings.auto_upgrade_stats)
        self.chk_bp = QCheckBox("Авто Battle Pass")
        self.chk_bp.setChecked(self.app_settings.auto_claim_bp)
        self.chk_cheater = QCheckBox("Режим «Читерский» — боссы без проверки статов (риск бана)")
        self.chk_cheater.setChecked(self.app_settings.cheater_wave_mode)
        cheater_hint = QLabel(
            "По умолчанию включён человеческий режим: бот смотрит на твои характеристики (жизнь, урон, скорость удара и т.д.) и на "
            "параметры босса. Если по этим данным победа маловероятна, бот намеренно проигрывает бой, качается и снова пробует, пока не "
            "станет сильнее — обычные волны без босса при этом проходятся как обычно.\n\n"
            "Читерский режим это отменяет: статы не учитываются, любой босс считается побеждённым, даже если персонаж совсем слабый. "
            "Так можно быстро улететь по волнам, но серверу это легче заметить, риск бана выше. Имеет смысл включать ненадолго, "
            "дойти до нужной волны и снова выключить."
        )
        cheater_hint.setWordWrap(True)
        cheater_hint.setStyleSheet("color: #8a8a8a; font-size: 11px; max-width: 560px;")
        f.addRow("Пауза мин (с)", self.set_sleep_min)
        f.addRow("Пауза макс (с)", self.set_sleep_max)
        f.addRow(self.chk_chest)
        f.addRow(self.chk_equip)
        f.addRow(self.chk_stats)
        f.addRow(self.chk_bp)
        f.addRow(self.chk_cheater)
        layout.addLayout(f)
        layout.addWidget(cheater_hint)
        self.btn_save_settings = QPushButton("Сохранить настройки")
        self.btn_save_settings.setObjectName("primary")
        layout.addWidget(self.btn_save_settings)
        layout.addStretch(1)
        self.btn_save_settings.clicked.connect(self._save_settings)

    def _reload_proxy_list(self) -> None:
        self.proxy_list.clear()
        by_norm = self._account_names_by_proxy_norm()
        for p in self.proxy_pool.list_all():
            pn = normalize_proxy_input(p) or p
            names = by_norm.get(pn, [])
            if names:
                text = f"{p}  →  {', '.join(names)}"
            else:
                text = f"{p}  ·  свободно"
            it = QListWidgetItem(text)
            it.setData(Qt.ItemDataRole.UserRole, p)
            it.setToolTip(pn)
            self.proxy_list.addItem(it)
        n = self.proxy_pool.count()
        free_n = self._free_proxy_count()
        self.lbl_proxy_stats.setText(f"В пуле строк: {n} · свободно: {free_n}")
        self._update_metric_proxy_only()

    def _proxy_add_bulk(self) -> None:
        text = self.proxy_bulk_input.toPlainText()
        added, dup = self.proxy_pool.add_raw_text(text, normalize_proxy_input)
        merged = self._dedupe_proxy_pool()
        self.proxy_bulk_input.clear()
        self._reload_proxy_list()
        QMessageBox.information(
            self,
            "Пул",
            f"Добавлено: {added}, дубликатов при вводе не записано: {dup}. "
            f"Удалено лишних повторов в общем списке: {merged}.",
        )

    def _proxy_delete_selected(self) -> None:
        idx = [self.proxy_list.row(i) for i in self.proxy_list.selectedItems()]
        if not idx:
            return
        self.proxy_pool.remove_at_indices(idx)
        self._reload_proxy_list()

    def _proxy_clear_all(self) -> None:
        if self.proxy_pool.count() == 0:
            return
        free_n = self._free_proxy_count()
        if free_n == 0:
            QMessageBox.information(
                self,
                "Пул",
                "В пуле нет свободных прокси — все строки совпадают с закреплёнными за аккаунтами. "
                "Удалить такие прокси из списка нельзя, иначе потеряется привязка. "
                "Если нужно убрать прокси у аккаунта, сначала смените или обнулите его у аккаунта в данных.",
            )
            return
        if (
            QMessageBox.question(
                self,
                "Пул",
                f"Удалить из пула только свободные прокси (≈{free_n} строк)?\n\n"
                "Строки, закреплённые за аккаунтами, останутся — боты не останутся без привязки в пуле.",
            )
            != QMessageBox.StandardButton.Yes
        ):
            return
        removed = self.proxy_pool.remove_unassigned_only(normalize_proxy_input, self._occupied_proxy_norms())
        self._reload_proxy_list()
        self._refresh_proxy_sidebar_counts()
        self._update_metrics()
        QMessageBox.information(
            self,
            "Пул",
            f"Удалено свободных строк: {removed}. Прокси с аккаунтами сохранены в списке.",
        )

    def _on_settings_sleep_min_changed(self, v: int) -> None:
        self.set_sleep_max.setMinimum(v)
        if self.set_sleep_max.value() < v:
            self.set_sleep_max.setValue(v)

    def _save_settings(self) -> None:
        mn = int(self.set_sleep_min.value())
        mx = int(self.set_sleep_max.value())
        if mx < mn:
            mx = mn
            self.set_sleep_max.setValue(mx)
        self.app_settings = AppSettings(
            sleep_min_seconds=float(mn),
            sleep_max_seconds=float(mx),
            auto_open_chests=self.chk_chest.isChecked(),
            auto_equip=self.chk_equip.isChecked(),
            auto_upgrade_stats=self.chk_stats.isChecked(),
            auto_claim_bp=self.chk_bp.isChecked(),
            cheater_wave_mode=self.chk_cheater.isChecked(),
        )
        self.app_settings_store.save(self.app_settings)
        for acc in self.account_store.list_accounts():
            apply_app_settings(acc, self.app_settings)
            self.account_store.upsert_account(acc)
        self._load_accounts()
        QMessageBox.warning(
            self,
            "Настройки",
            "Чтобы новые настройки применились к бою, нажми «Остановить всех», затем «Запустить всех».",
        )

    def _occupied_proxy_norms(self) -> set[str]:
        return occupied_proxy_norms_from_accounts(self._accounts_list)

    def _account_names_by_proxy_norm(self) -> dict[str, list[str]]:
        m: dict[str, list[str]] = {}
        for a in self._accounts_list:
            n = normalize_proxy_input(a.http_proxy)
            if not n:
                continue
            m.setdefault(n, []).append(a.name)
        return m

    def _sync_account_proxies_into_pool(self) -> None:
        for a in self._accounts_list:
            n = normalize_proxy_input(a.http_proxy)
            if n:
                self.proxy_pool.ensure_in_pool(normalize_proxy_input, n)

    def _dedupe_proxy_pool(self) -> int:
        return self.proxy_pool.dedupe_normalized(normalize_proxy_input)

    def _free_proxy_count(self) -> int:
        return count_free_proxies_in_pool(self.proxy_pool, self._occupied_proxy_norms())

    def _refresh_proxy_sidebar_counts(self) -> None:
        n = self.proxy_pool.count()
        free_n = self._free_proxy_count()
        self.lbl_node_na.setText(f"Прокси в пуле: {n} · свободно: {free_n}")
        self.lbl_node_eu.setText(f"Аккаунтов: {len(self._accounts_list)}")
        running = sum(1 for s in self.snapshots.values() if s.running)
        self.lbl_node_ap.setText(f"Онлайн: {running}")

    def _periodic_light_refresh(self) -> None:
        """Только сводка; карточки обновляются по событиям воркера и опросу деталей."""
        self._update_metrics()
        n_acc = len(self._accounts_list)
        run = sum(1 for s in self.snapshots.values() if s.running)
        if hasattr(self, "lbl_fleet_meta"):
            self.lbl_fleet_meta.setText(f"{n_acc} аккаунт(ов) · в работе: {run}")
        self._refresh_proxy_sidebar_counts()

    def _update_metrics(self) -> None:
        running = sum(1 for s in self.snapshots.values() if s.running)
        faults = sum(1 for s in self.snapshots.values() if s.last_error)
        free_px = self._free_proxy_count()
        ch_sum = 0.0
        for a in self._accounts_list:
            s = self.snapshots.get(a.account_id)
            if s and s.balance is not None:
                ch_sum += float(s.balance)
        self.lbl_m_active.setText(str(running))
        self.lbl_m_proxy.setText(str(free_px))
        self.lbl_m_ch.setText(format_ch_amount(ch_sum))
        self.lbl_m_faults.setText(str(faults))

    def _update_metric_proxy_only(self) -> None:
        self.lbl_m_proxy.setText(str(self._free_proxy_count()))

    def _clear_cards(self) -> None:
        self.cards.clear()
        while self.cards_grid.count():
            it = self.cards_grid.takeAt(0)
            w = it.widget()
            if w:
                w.deleteLater()

    def _build_cards(self) -> None:
        self._clear_cards()
        for acc in self._accounts_list:
            card = AccountCard(acc, self.cards_container)
            card.start_clicked.connect(self._start_account)
            card.stop_clicked.connect(self._stop_account)
            card.refresh_clicked.connect(self._refresh_account_details)
            self.cards[acc.account_id] = card
        self._relayout_fleet_cards()

    def _load_accounts(self) -> None:
        self._accounts_list = self.account_store.list_accounts()
        alive = {a.account_id for a in self._accounts_list}
        self.account_cache_store.prune_removed_accounts(alive)
        for aid in list(self.snapshots.keys()):
            if aid not in alive:
                del self.snapshots[aid]
        for acc in self._accounts_list:
            if acc.account_id not in self.snapshots:
                self.snapshots[acc.account_id] = self.state_store.load_snapshot(acc.account_id) or BotSnapshot(
                    account_id=acc.account_id
                )
        self._sync_account_proxies_into_pool()
        self._dedupe_proxy_pool()
        self._build_cards()
        self._refresh_cards()
        self._reload_proxy_list()
        self._refresh_proxy_sidebar_counts()
        self._sync_all_account_manager_cache_batch()
        if hasattr(self, "stack") and self.stack.currentIndex() == 2:
            self._fill_accounts_manager_table()

    def _find_account(self, account_id: str) -> AccountConfig | None:
        for a in self._accounts_list:
            if a.account_id == account_id:
                return a
        return None

    def _live_stats_patch(
        self,
        acc: AccountConfig,
        *,
        wallet_row: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        snap = self.snapshots.get(acc.account_id)
        d = self.details_cache.get(acc.account_id, {})
        if wallet_row is not None:
            w = wallet_row
        else:
            w = self.wallet_store.get_wallet(acc.account_id) or {}
        patch: dict[str, Any] = {
            "name": acc.name,
            "wallet_address": w.get("wallet_address") or None,
            "proxy": acc.http_proxy or None,
        }
        if snap:
            if snap.balance is not None:
                patch["balance"] = float(snap.balance)
            if snap.wave is not None:
                patch["wave"] = snap.wave
            if snap.level is not None:
                patch["level"] = snap.level
            patch["running"] = snap.running
            if snap.total_earned is not None:
                patch["total_earned"] = float(snap.total_earned)
        if d.get("level") is not None:
            patch["level"] = d["level"]
        return patch

    def _sync_account_manager_cache_for(self, acc: AccountConfig) -> None:
        self.account_cache_store.upsert_partial(acc.account_id, self._live_stats_patch(acc))

    def _sync_all_account_manager_cache_batch(self) -> None:
        wallets = self.wallet_store.get_all_wallets()
        patches = {
            acc.account_id: self._live_stats_patch(
                acc, wallet_row=wallets.get(acc.account_id) or {}
            )
            for acc in self._accounts_list
        }
        self.account_cache_store.upsert_many_partials(patches)

    def _account_row_view(
        self,
        acc: AccountConfig,
        *,
        cache_rows: dict[str, dict[str, Any]] | None = None,
        wallets_by_id: dict[str, dict[str, str]] | None = None,
    ) -> dict[str, Any]:
        if cache_rows is None:
            cache_rows = self.account_cache_store.get_rows()
        cached = cache_rows.get(acc.account_id, {})
        snap = self.snapshots.get(acc.account_id)
        d = self.details_cache.get(acc.account_id, {})
        if wallets_by_id is not None:
            w = wallets_by_id.get(acc.account_id) or {}
        else:
            w = self.wallet_store.get_wallet(acc.account_id) or {}
        level = d.get("level")
        if level is None:
            level = cached.get("level")
        if snap and snap.level is not None:
            level = snap.level
        balance = None
        if snap and snap.balance is not None:
            balance = float(snap.balance)
        elif cached.get("balance") is not None:
            balance = float(cached["balance"])
        wave = None
        if snap and snap.wave is not None:
            wave = snap.wave
        elif cached.get("wave") is not None:
            wave = int(cached["wave"]) if isinstance(cached["wave"], (int, float)) else cached["wave"]
        running = bool(snap.running) if snap else bool(cached.get("running"))
        total_earned = None
        if snap and snap.total_earned is not None:
            total_earned = float(snap.total_earned)
        elif cached.get("total_earned") is not None:
            total_earned = float(cached["total_earned"])
        wa = w.get("wallet_address") or cached.get("wallet_address") or ""
        pk = w.get("private_key_b58") or ""
        balance_str = format_ch_amount(balance) if balance is not None else "—"
        total_earned_str = format_ch_amount(total_earned) if total_earned is not None else ""
        cu = str(cached.get("updated_at") or "")
        return {
            "nickname": acc.name,
            "account_id": acc.account_id,
            "proxy": acc.http_proxy or "",
            "wallet_address": wa,
            "private_key": pk,
            "balance": balance,
            "balance_str": balance_str,
            "level": level,
            "wave": wave,
            "total_earned": total_earned,
            "total_earned_str": total_earned_str,
            "running": running,
            "cache_updated": cu,
        }

    def _fill_accounts_manager_table(self) -> None:
        if not hasattr(self, "acct_table"):
            return
        cache_rows = self.account_cache_store.get_rows()
        wallets_by_id = self.wallet_store.get_all_wallets()
        ban_bg = QBrush(QColor("#33191c"))
        ban_fg = QBrush(QColor("#ffc8c4"))
        stripe_a = QBrush(QColor("#1c1c1c"))
        stripe_b = QBrush(QColor("#232323"))
        fg_normal = QBrush(QColor("#eaeaea"))
        self.acct_table.setUpdatesEnabled(False)
        try:
            self.acct_table.setRowCount(len(self._accounts_list))
            for r, acc in enumerate(self._accounts_list):
                snap_row = self.snapshots.get(acc.account_id)
                row_banned = bool(snap_row and snap_row.account_banned)
                rw = self._account_row_view(
                    acc, cache_rows=cache_rows, wallets_by_id=wallets_by_id
                )
                px = rw.get("proxy") or ""
                px_show = _short_proxy_cell(px, 42)
                vals = [
                    str(r + 1),
                    rw["nickname"],
                    _short_account_id_cell(rw["account_id"]),
                    _short_sol_addr(rw.get("wallet_address") or ""),
                    rw["balance_str"],
                    str(rw["level"]) if rw["level"] is not None else "",
                    str(rw["wave"]) if rw["wave"] is not None else "",
                    "да" if rw.get("running") else "нет",
                    "",
                ]
                row_stripe = stripe_a if r % 2 == 0 else stripe_b
                for c, val in enumerate(vals):
                    it = QTableWidgetItem(str(val))
                    if c == 0:
                        it.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                    if c == 2:
                        it.setToolTip(rw["account_id"])
                    if row_banned:
                        it.setBackground(ban_bg)
                        it.setForeground(ban_fg)
                    else:
                        it.setBackground(row_stripe)
                        it.setForeground(fg_normal)
                    self.acct_table.setItem(r, c, it)

                proxy_wrap = QWidget()
                ph = QHBoxLayout(proxy_wrap)
                ph.setContentsMargins(4, 2, 4, 2)
                ph.setSpacing(6)
                lbl = QLabel(px_show or "—")
                lbl.setToolTip(px or "Прокси не задан")
                lbl.setStyleSheet(
                    "color: #ffc8c4; font-size: 12px;"
                    if row_banned
                    else "color: #d8d8d8; font-size: 12px;"
                )
                btn = QPushButton("Изм.")
                btn.setObjectName("acctProxyBtn")
                btn.setFixedSize(44, 24)
                btn.setToolTip("Сменить прокси аккаунта")
                btn.clicked.connect(partial(self._change_account_proxy, acc.account_id))
                ph.addWidget(lbl, 1, Qt.AlignmentFlag.AlignVCenter)
                ph.addWidget(btn, 0, Qt.AlignmentFlag.AlignVCenter)
                if row_banned:
                    proxy_wrap.setStyleSheet("background-color: #33191c; border-radius: 6px;")
                else:
                    stripe_hex = "#1c1c1c" if r % 2 == 0 else "#232323"
                    proxy_wrap.setStyleSheet(f"background-color: {stripe_hex}; border-radius: 6px;")
                self.acct_table.setCellWidget(r, 8, proxy_wrap)
                self.acct_table.setRowHeight(r, self.ACCT_TABLE_ROW_HEIGHT)
        finally:
            self.acct_table.setUpdatesEnabled(True)
        self.acct_table.resizeColumnToContents(0)

    def _available_pool_proxies_for_account(self, account_id: str) -> list[str]:
        acc = self._find_account(account_id)
        if not acc:
            return []
        current = normalize_proxy_input(acc.http_proxy)
        used = self._occupied_proxy_norms()
        if current in used:
            used.remove(current)
        out: list[str] = []
        seen: set[str] = set()
        for p in self.proxy_pool.list_all():
            n = normalize_proxy_input(p)
            if not n:
                continue
            if n in used:
                continue
            if n in seen:
                continue
            seen.add(n)
            out.append(n)
        if current and current not in seen:
            out.insert(0, current)
        return out

    def _change_account_proxy(self, account_id: str) -> None:
        acc = self._find_account(account_id)
        if not acc:
            return
        options = self._available_pool_proxies_for_account(account_id)
        dlg = ChangeProxyDialog(acc.http_proxy, options, self)
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return
        new_proxy = dlg.chosen_proxy()
        if not new_proxy:
            QMessageBox.warning(self, "Прокси", "Прокси пустой или некорректный.")
            return
        occ = self._occupied_proxy_norms()
        cur = normalize_proxy_input(acc.http_proxy)
        if cur in occ:
            occ.remove(cur)
        if new_proxy in occ:
            QMessageBox.warning(
                self,
                "Прокси",
                "Этот прокси уже закреплён за другим аккаунтом (один прокси — один аккаунт).",
            )
            return
        updated = replace(acc, http_proxy=new_proxy)
        self.account_store.upsert_account(updated)
        self.proxy_pool.ensure_in_pool(normalize_proxy_input, new_proxy)
        w = self.workers.get(account_id)
        if w:
            w.account.http_proxy = new_proxy
        self._load_accounts()

    def _delete_banned_accounts_clicked(self) -> None:
        banned_ids = [aid for aid, snap in self.snapshots.items() if snap.account_banned]
        if not banned_ids:
            QMessageBox.information(self, "Аккаунты", "Забаненных аккаунтов нет.")
            return
        n = len(banned_ids)
        if (
            QMessageBox.question(
                self,
                "Удаление",
                f"Удалить {n} забаненн(ых) аккаунт(ов)? Будут удалены данные аккаунта, кошелёк, состояние и закреплённый за ним прокси из пула. Отменить это нельзя.",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No,
            )
            != QMessageBox.StandardButton.Yes
        ):
            return
        for aid in banned_ids:
            acc = self._find_account(aid)
            proxy_norm = normalize_proxy_input(acc.http_proxy) if acc else None
            self._stop_account(aid)
            self.workers.pop(aid, None)
            self.account_store.delete_account(aid)
            self.wallet_store.remove_wallet(aid)
            self.state_store.delete_snapshot(aid)
            self.snapshots.pop(aid, None)
            self.details_cache.pop(aid, None)
            if proxy_norm:
                self.proxy_pool.remove_by_normalized(normalize_proxy_input, proxy_norm)
        self._load_accounts()

    def _export_accounts_clicked(self) -> None:
        cache_rows = self.account_cache_store.get_rows()
        wallets_by_id = self.wallet_store.get_all_wallets()
        rows = [
            self._account_row_view(
                a, cache_rows=cache_rows, wallets_by_id=wallets_by_id
            )
            for a in self._accounts_list
        ]
        ExportAccountsDialog(rows, self).exec()

    def _append_log(self, line: str) -> None:
        self.log_box.appendPlainText(line)
        sb = self.log_box.verticalScrollBar()
        sb.setValue(sb.maximum())

    def _on_worker_log(self, level: str, message: str) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        if "Победа в волне" in message or "Награда ·" in message:
            self._append_log(f"[{ts}]  ★  {message}")
        else:
            self._append_log(f"[{ts}] {level.upper():7s} {message}")

    def _on_worker_event(self, ev: BotEvent) -> None:
        snap = self.snapshots.get(ev.account_id) or BotSnapshot(account_id=ev.account_id)
        persist = False
        refresh = False

        if ev.event_type == "started":
            snap.running = True
            snap.last_error = None
            persist = True
            refresh = True
        elif ev.event_type == "stopped":
            snap.running = False
            persist = True
            refresh = True
        elif ev.event_type == "error":
            snap.last_error = str(ev.payload.get("message"))
            snap.running = False
            if ev.payload.get("account_banned"):
                snap.account_banned = True
            persist = True
            refresh = True
        elif ev.event_type == "log":
            msg = str(ev.payload.get("message", ""))
            snap.last_message = msg
            snap.updated_at = datetime.utcnow()
            self.snapshots[ev.account_id] = snap
            # «Победа в волне» сразу дублируется событием wave_done с красивым многострочным текстом — не дёргаем всю сетку.
            if "Победа в волне" in msg:
                return
            card = self.cards.get(ev.account_id)
            acc = self._find_account(ev.account_id)
            if card and acc:
                card.update_card(acc, snap, self.details_cache.get(ev.account_id, {}))
            return
        elif ev.event_type == "wave_done":
            snap.wave = ev.payload.get("wave")
            snap.floor = ev.payload.get("floor")
            snap.room = ev.payload.get("room")
            snap.balance = ev.payload.get("balance")
            snap.total_earned = ev.payload.get("total_earned")
            snap.last_message = _wave_done_activity_text(ev.payload)
            persist = True
            refresh = True
        else:
            return

        snap.updated_at = datetime.utcnow()
        self.snapshots[ev.account_id] = snap
        if persist:
            to_save = replace(snap)
            threading.Thread(
                target=lambda s=to_save: self.state_store.save_snapshot(s),
                daemon=True,
            ).start()
        acc_ev = self._find_account(ev.account_id)
        if acc_ev and persist:
            self._sync_account_manager_cache_for(acc_ev)
        if refresh:
            self._schedule_ui_refresh()
            if hasattr(self, "stack") and self.stack.currentIndex() == 2:
                self._fill_accounts_manager_table()

    def _refresh_cards(self) -> None:
        self._update_metrics()
        n_acc = len(self._accounts_list)
        run = sum(1 for s in self.snapshots.values() if s.running)
        if hasattr(self, "lbl_fleet_meta"):
            self.lbl_fleet_meta.setText(f"{n_acc} аккаунт(ов) · в работе: {run}")
        for acc in self._accounts_list:
            card = self.cards.get(acc.account_id)
            if not card:
                continue
            snap = self.snapshots.get(acc.account_id) or BotSnapshot(account_id=acc.account_id)
            details = self.details_cache.get(acc.account_id, {})
            card.update_card(acc, snap, details)

    def _refresh_running_details(self) -> None:
        if self._details_poll_busy:
            return
        accs = [
            a
            for a in self._accounts_list
            if self.snapshots.get(a.account_id) and self.snapshots[a.account_id].running
        ]
        if not accs:
            return
        self._details_poll_busy = True

        def run() -> None:
            results: dict = {}
            try:
                for acc in accs:
                    sp = self.snapshots.get(acc.account_id)
                    results[acc.account_id] = _fetch_account_details_dict(
                        acc, account_banned=bool(sp and sp.account_banned)
                    )
            finally:
                self._sig_detail_poll.emit(results)

        threading.Thread(target=run, daemon=True).start()

    def _on_detail_poll_done(self, results: dict) -> None:
        self._details_poll_busy = False
        for aid, d in results.items():
            self.details_cache[aid] = d
            acc = self._find_account(aid)
            if acc:
                self._sync_account_manager_cache_for(acc)
        self._refresh_cards()
        if hasattr(self, "stack") and self.stack.currentIndex() == 2:
            self._fill_accounts_manager_table()

    def _on_detail_manual_done(self, results: dict) -> None:
        for aid, d in results.items():
            self.details_cache[aid] = d
            acc = self._find_account(aid)
            if acc:
                self._sync_account_manager_cache_for(acc)
        self._refresh_cards()
        if hasattr(self, "stack") and self.stack.currentIndex() == 2:
            self._fill_accounts_manager_table()

    def _refresh_account_details(self, account_id: str) -> None:
        acc = self._find_account(account_id)
        if not acc:
            return

        def run() -> None:
            sp = self.snapshots.get(acc.account_id)
            results = {
                acc.account_id: _fetch_account_details_dict(
                    acc, account_banned=bool(sp and sp.account_banned)
                )
            }
            self._sig_detail_manual.emit(results)

        threading.Thread(target=run, daemon=True).start()

    def _start_account(self, account_id: str) -> None:
        if account_id in self.workers and self.workers[account_id].snapshot.running:
            return
        acc = self._find_account(account_id)
        if not acc:
            return
        snap_pre = self.snapshots.get(account_id) or BotSnapshot(account_id=account_id)
        if snap_pre.account_banned:
            QMessageBox.warning(
                self,
                "Аккаунт",
                "Аккаунт заблокирован на сервере (ACCOUNT_BANNED). Запуск бота с него отключён.",
            )
            return
        acc_run = replace(acc, cheater_wave_mode=self.app_settings.cheater_wave_mode)
        worker = BotWorker(
            project_root=self.root,
            account=acc_run,
            on_event=lambda ev: self.bridge.event_signal.emit(ev),
            on_log=lambda level, msg: self.bridge.log_signal.emit(level, f"[{acc.name}] {msg}"),
            snapshot_seed=snap_pre,
        )
        self.workers[account_id] = worker
        worker.start()
        snap = self.snapshots.get(account_id) or BotSnapshot(account_id=account_id)
        snap.running = True
        snap.last_error = None
        self.snapshots[account_id] = snap
        threading.Thread(
            target=lambda s=replace(snap): self.state_store.save_snapshot(s),
            daemon=True,
        ).start()
        self._sync_account_manager_cache_for(acc)
        self._schedule_ui_refresh()
        if hasattr(self, "stack") and self.stack.currentIndex() == 2:
            self._fill_accounts_manager_table()

    def _stop_account(self, account_id: str) -> None:
        worker = self.workers.get(account_id)
        if worker:
            worker.stop()

    def _resume_all(self) -> None:
        for acc in self._accounts_list:
            self._start_account(acc.account_id)

    def _stop_all(self) -> None:
        for acc in self._accounts_list:
            self._stop_account(acc.account_id)
        for aid, snap in list(self.snapshots.items()):
            if not snap.running:
                continue
            new_snap = replace(snap, running=False, updated_at=datetime.utcnow())
            self.snapshots[aid] = new_snap
            to_save = replace(new_snap)
            threading.Thread(
                target=lambda s=to_save: self.state_store.save_snapshot(s),
                daemon=True,
            ).start()
        for acc in self._accounts_list:
            self._sync_account_manager_cache_for(acc)
        self._schedule_ui_refresh()
        if hasattr(self, "stack") and self.stack.currentIndex() == 2:
            self._fill_accounts_manager_table()

    def _list_unused_proxy_lines_raw(self) -> list[str]:
        occ = self._occupied_proxy_norms()
        out: list[str] = []
        for line in self.proxy_pool.list_all():
            n = normalize_proxy_input(line)
            if n and n not in occ:
                out.append(str(line).strip())
        return out

    def _run_bulk_register(self, rows: list[tuple[str, str]], character_class: str, autostart: bool) -> None:
        count = len(rows)
        free_n = self._free_proxy_count()
        if count > free_n:
            QMessageBox.warning(
                self,
                "Пул",
                f"Свободных прокси: {free_n}, запрошено аккаунтов: {count}. Добавь прокси в пул или освободи занятые.",
            )
            return
        prog = QProgressDialog("Массовая регистрация…", "Отмена", 0, count, self)
        prog.setWindowModality(Qt.WindowModality.WindowModal)
        prog.setMinimumDuration(0)
        ok_n = 0
        errors: list[str] = []
        registered_ids: list[str] = []
        occupied = set(self._occupied_proxy_norms())
        for i, (nick, proxy_line) in enumerate(rows):
            if prog.wasCanceled():
                break
            prog.setValue(i)
            prog.setLabelText(f"Аккаунт {i + 1} / {count}: {nick}")
            QApplication.processEvents()
            px_n = normalize_proxy_input(proxy_line)
            if not px_n or px_n in occupied:
                errors.append(f"Строка {i + 1}: прокси недоступен или занят")
                self._append_log(f"[REG] FAIL строка {i + 1}: прокси недоступен")
                prog.setValue(i + 1)
                continue
            try:
                reg = register_new_account_api_only(
                    proxy=proxy_line.strip(),
                    client_version=APP_CLIENT_VERSION,
                    nickname=nick.strip(),
                    character_class=character_class,
                )
                apply_app_settings(reg.account, self.app_settings)
                pn = normalize_proxy_input(reg.account.http_proxy) or px_n
                if pn:
                    occupied.add(pn)
                    self.proxy_pool.ensure_in_pool(normalize_proxy_input, pn)
                self.account_store.upsert_account(reg.account)
                self.wallet_store.put_wallet(reg.account.account_id, reg.wallet_address, reg.private_key_b58)
                ok_n += 1
                registered_ids.append(reg.account.account_id)
                self._append_log(f"[REG] OK {reg.account.name} (прокси в пуле, закреплён за аккаунтом)")
            except Exception as e:
                msg = str(e)
                errors.append(msg[:200])
                self._append_log(f"[REG] FAIL {msg[:120]}")
            prog.setValue(i + 1)
        prog.setValue(count)
        self._load_accounts()
        self._reload_proxy_list()
        QMessageBox.information(
            self,
            "Массовая регистрация",
            f"Успешно: {ok_n} из {count}.\nОшибок: {len(errors)}." + (f"\n\nПоследняя: {errors[-1]}" if errors else ""),
        )
        if autostart and registered_ids:
            for aid in registered_ids:
                self._start_account(aid)

    def _add_account(self) -> None:
        free_n = self._free_proxy_count()
        pool_total = self.proxy_pool.count()
        dlg = AddAccountDialog(free_n, pool_total, self)
        dlg.attach_pool_peek(
            lambda: self.proxy_pool.peek_first_unused(normalize_proxy_input, self._occupied_proxy_norms())
        )
        dlg.attach_bulk_preview(self._list_unused_proxy_lines_raw, self._occupied_proxy_norms)
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return
        try:
            occ = self._occupied_proxy_norms()
            if dlg.mode() == "manual":
                name = dlg.manual_name.text().strip() or "manual"
                token = dlg.manual_token.text().strip()
                if not token:
                    raise RuntimeError("Пустой token")
                proxy = normalize_proxy_input(dlg.manual_proxy.text())
                if proxy and proxy in occ:
                    raise RuntimeError("Этот прокси уже закреплён за другим аккаунтом (один прокси — один аккаунт).")
                account = AccountConfig(
                    account_id=f"acc_{abs(hash(token)) % 10_000_000}",
                    name=name,
                    bearer_token=token,
                    client_version=APP_CLIENT_VERSION,
                    http_proxy=proxy,
                    browser_profile=generate_browser_profile(),
                )
                apply_app_settings(account, self.app_settings)
                self.account_store.upsert_account(account)
                if proxy:
                    self.proxy_pool.ensure_in_pool(normalize_proxy_input, proxy)
            elif dlg.mode() == "register":
                proxy = normalize_proxy_input(dlg.reg_proxy.text())
                if not proxy:
                    raise RuntimeError(
                        "Прокси пустой или в неверном формате. Пример: host:port:user:pass"
                    )
                if proxy in occ:
                    raise RuntimeError("Этот прокси уже закреплён за другим аккаунтом.")
                reg: RegistrationResult = register_new_account_api_only(
                    proxy=proxy,
                    client_version=APP_CLIENT_VERSION,
                    nickname=dlg.reg_nick.text().strip() or None,
                    character_class=dlg.reg_class.currentText(),
                )
                apply_app_settings(reg.account, self.app_settings)
                self.account_store.upsert_account(reg.account)
                self.wallet_store.put_wallet(reg.account.account_id, reg.wallet_address, reg.private_key_b58)
                self.proxy_pool.ensure_in_pool(normalize_proxy_input, proxy)
            else:
                bulk_rows = dlg.get_bulk_rows()
                if not bulk_rows:
                    return
                cl, auto = dlg.bulk_options()
                self._run_bulk_register(bulk_rows, cl, auto)
                return

            self._load_accounts()
        except Exception as e:
            msg = str(e)
            if "IP_BANNED|Cheating" in msg:
                msg = "Прокси заблокирован античитом (IP_BANNED|Cheating). Попробуй другой прокси."
            QMessageBox.critical(self, "Ошибка добавления аккаунта", f"{msg}\n\n{traceback.format_exc()}")


def run_ui_app() -> None:
    app = QApplication(sys.argv)
    fusion = QStyleFactory.create("Fusion")
    if fusion:
        app.setStyle(fusion)
    app.setStyleSheet(THEME_QSS)
    font = QFont("Segoe UI", 10)
    app.setFont(font)

    root = Path(__file__).resolve().parent
    ensure_project_data_initialized(root)
    win = MainWindow(root)
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    run_ui_app()
