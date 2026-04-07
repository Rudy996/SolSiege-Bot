"""
Локальные решатели капчи SolSiege (math, word, color, emoji).
"""
import json
import re


def try_local_math_answer(question: str) -> str | None:
    """
    Простые выражения SolSiege math-капчи: «3 x 7 = ?», «10 + 5 = ?» и т.д.
    Без eval: только два операнда и одна операция.
    """
    if not question or not isinstance(question, str):
        return None
    q = question.strip()
    q = q.replace("×", "*").replace("÷", "/")
    q = re.sub(r"\s*x\s*", "*", q, flags=re.IGNORECASE)
    q = re.sub(r"\s+", " ", q)

    def _mul(m):
        return str(int(m.group(1)) * int(m.group(2)))

    def _add(m):
        return str(int(m.group(1)) + int(m.group(2)))

    def _sub(m):
        return str(int(m.group(1)) - int(m.group(2)))

    def _div(m):
        a, b = int(m.group(1)), int(m.group(2))
        if b == 0:
            return None
        return str(a // b)

    for pattern, fn in (
        (r"^(\d+)\s*\*\s*(\d+)\s*=\s*\?\s*$", _mul),
        (r"^(\d+)\s*\+\s*(\d+)\s*=\s*\?\s*$", _add),
        (r"^(\d+)\s*-\s*(\d+)\s*=\s*\?\s*$", _sub),
        (r"^(\d+)\s*/\s*(\d+)\s*=\s*\?\s*$", _div),
    ):
        m = re.match(pattern, q)
        if m:
            return fn(m)
    return None


def try_local_word_captcha(question: str) -> str | None:
    """
    word: текст вроде Type \"BOSS\" to continue — ответом идёт слово из кавычек (как в игре).
    """
    if not question or not isinstance(question, str):
        return None
    q = question.strip()
    patterns = (
        r'(?i)type\s+"([^"]+)"',
        r"(?i)type\s+'([^']+)'",
        r"(?i)type\s+\u201c([^\u201d]+)\u201d",
        r'(?i)enter\s+"([^"]+)"',
        r'(?i)repeat\s+"([^"]+)"',
    )
    for pat in patterns:
        m = re.search(pat, q)
        if m:
            return m.group(1).strip()
    return None


def _hex_rgb(hex_color: str) -> tuple[int, int, int]:
    h = hex_color.strip().lstrip("#")
    if len(h) < 6:
        raise ValueError("hex")
    return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)


def _rgb_hue(r: int, g: int, b: int) -> float:
    """Оттенок 0..360 для сравнения «какой это цвет по кругу»."""
    r, g, b = r / 255.0, g / 255.0, b / 255.0
    mx = max(r, g, b)
    mn = min(r, g, b)
    if mx == mn:
        return 0.0
    d = mx - mn
    if mx == r:
        h = ((g - b) / d) % 6
    elif mx == g:
        h = (b - r) / d + 2
    else:
        h = (r - g) / d + 4
    return (h * 60.0) % 360.0


_COLOR_KEYWORD_HUE = {
    "red": 0.0,
    "scarlet": 2.0,
    "orange": 30.0,
    "amber": 38.0,
    "gold": 45.0,
    "yellow": 55.0,
    "lime": 88.0,
    "green": 125.0,
    "emerald": 145.0,
    "teal": 175.0,
    "cyan": 185.0,
    "sky": 200.0,
    "blue": 220.0,
    "navy": 240.0,
    "indigo": 255.0,
    "purple": 275.0,
    "violet": 280.0,
    "magenta": 300.0,
    "pink": 330.0,
    "rose": 350.0,
    "brown": 25.0,
    "tan": 35.0,
    "grey": 0.0,
    "gray": 0.0,
    "black": 0.0,
    "white": 0.0,
}


def _hue_distance(a: float, b: float) -> float:
    d = abs(a - b) % 360.0
    return min(d, 360.0 - d)


def try_local_color_captcha(captcha_question: str) -> str | None:
    """
    color: JSON {"ask":"Select the orange color","options":["#f97316",...]}.
    Ищем цветовое слово в ask, сравниваем оттенки hex-кодов.
    """
    if not captcha_question:
        return None
    try:
        v = (
            json.loads(captcha_question)
            if isinstance(captcha_question, str)
            else captcha_question
        )
    except (json.JSONDecodeError, TypeError):
        return None
    ask = (v.get("ask") or "").lower()
    opts = v.get("options") or []
    if len(opts) < 2:
        return None
    best_word = None
    best_wlen = 0
    for word in _COLOR_KEYWORD_HUE:
        if word in ask and len(word) > best_wlen:
            best_wlen = len(word)
            best_word = word
    if not best_word:
        return None
    target = _COLOR_KEYWORD_HUE[best_word]
    best_i = 0
    best_d = 1e9
    for i, opt in enumerate(opts):
        if not isinstance(opt, str) or not opt.startswith("#"):
            continue
        try:
            rgb = _hex_rgb(opt)
            h = _rgb_hue(*rgb)
        except (ValueError, IndexError):
            continue
        d = _hue_distance(h, target)
        if best_word in ("grey", "gray", "white", "black"):
            r, g, b = rgb
            mx, mn = max(r, g, b), min(r, g, b)
            sat = 0 if mx == 0 else (mx - mn) / mx
            lum = mx / 255.0
            if best_word == "gray" or best_word == "grey":
                d = abs(sat - 0.15) + _hue_distance(h, target) * 0.1
            elif best_word == "white":
                d = (1.0 - lum) + sat
            elif best_word == "black":
                d = lum + sat * 0.5
        if d < best_d:
            best_d = d
            best_i = i
    return str(best_i)


def try_local_emoji_captcha(captcha_question: str) -> str | None:
    """emoji: options — ключи вроде sword, shield; ищем совпадение в тексте ask."""
    if not captcha_question:
        return None
    try:
        v = (
            json.loads(captcha_question)
            if isinstance(captcha_question, str)
            else captcha_question
        )
    except (json.JSONDecodeError, TypeError):
        return None
    ask = (v.get("ask") or "").lower()
    opts = v.get("options") or []
    if not ask or len(opts) < 2:
        return None
    best_i = None
    best_len = -1
    for i, opt in enumerate(opts):
        o = str(opt).lower()
        if not o:
            continue
        if o in ask and len(o) > best_len:
            best_len = len(o)
            best_i = i
    if best_i is None:
        return None
    return str(best_i)
