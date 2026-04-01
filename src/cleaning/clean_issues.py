import re
import html
import pandas as pd
# ---------------------------
# 🔥 Precompiled regex (FAST)
# ---------------------------
CODE_BLOCK_RE = re.compile(r"```.*?```", re.DOTALL)
HTML_COMMENT_RE = re.compile(r"<!--.*?-->", re.DOTALL)
HTML_TAG_RE = re.compile(r"<[^>]+>")
URL_RE = re.compile(r"https?://\S+|www\.\S+")
INLINE_CODE_RE = re.compile(r"`([^`]*)`")
MARKDOWN_RE = re.compile(r"#+\s*|\*\*|__")
WHITESPACE_RE = re.compile(r"\s+")

# GitHub command matcher
GITHUB_CMD_RE = re.compile(r"^/(sig|kind|priority|area|lifecycle)\b", re.IGNORECASE)

def clean_github_issue(text):
    # ---------------------------
    # Handle nulls safely
    # ---------------------------
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return ""

    text = str(text)

    # ---------------------------
    # Remove heavy noise first
    # ---------------------------
    text = CODE_BLOCK_RE.sub(" ", text)
    text = HTML_COMMENT_RE.sub(" ", text)

    # ---------------------------
    # Normalize HTML
    # ---------------------------
    text = html.unescape(text)
    text = HTML_TAG_RE.sub(" ", text)

    # ---------------------------
    # Remove URLs
    # ---------------------------
    text = URL_RE.sub(" ", text)

    # ---------------------------
    # Remove GitHub bot lines
    # ---------------------------
    lines = text.split("\n")
    lines = [line for line in lines if not GITHUB_CMD_RE.match(line.strip())]
    text = "\n".join(lines)

    # ---------------------------
    # Keep inline code content
    # ---------------------------
    text = INLINE_CODE_RE.sub(r"\1", text)

    # ---------------------------
    # Remove basic markdown
    # ---------------------------
    text = MARKDOWN_RE.sub(" ", text)

    # ---------------------------
    # Normalize whitespace (FINAL)
    # ---------------------------
    text = WHITESPACE_RE.sub(" ", text).strip()

    return text