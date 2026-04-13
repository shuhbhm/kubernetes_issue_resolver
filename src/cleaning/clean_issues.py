"""
GitHub Issue Cleaning Utility (Optimized for Kubernetes Debugging)

Overview:
---------
This module provides a text cleaning function specifically designed for
GitHub issues in DevOps/Kubernetes contexts. Unlike generic text cleaners,
this implementation preserves critical debugging information such as logs,
error messages, and command outputs while removing formatting noise.

The goal is to prepare issue data for AI systems (e.g., RAG, embeddings, LLMs)
without losing important diagnostic signals.

Purpose:
--------
GitHub issues often contain mixed content:
- Valuable: logs, stack traces, commands, error messages
- Noisy: markdown, HTML, bot commands, excessive formatting

This cleaner removes noise while preserving technical context required for:
- Root cause analysis
- Issue classification
- AI-assisted debugging systems

Key Design Principle:
---------------------
"Remove formatting, NOT information"

Key Features:
-------------
1. Preserves Code Blocks (Critical):
   - Unlike aggressive cleaners, this function retains code blocks (```...```),
     which often contain logs, errors, and commands.
   - Optionally truncates very large blocks to prevent excessive length.

2. Removes HTML and Metadata Noise:
   - Strips HTML tags and comments
   - Decodes HTML entities into readable text

3. Filters GitHub Bot Commands:
   - Removes lines starting with:
       /sig, /kind, /priority, /area, /lifecycle
   - These are metadata and not useful for debugging

4. Markdown Cleanup:
   - Removes headings, bold markers, and formatting symbols
   - Preserves actual content

5. Inline Code Preservation:
   - Retains inline code content (e.g., `kubectl get pods`)
   - Removes only backtick formatting

6. URL Handling:
   - Removes URLs to reduce noise
   - Can be modified to retain important references if needed

7. Structure Preservation:
   - Maintains line breaks for readability and logical separation
   - Avoids flattening text into a single line

8. Whitespace Normalization:
   - Collapses excessive spaces while preserving structure
   - Prevents over-aggressive cleanup

9. Robust Null Handling:
   - Safely handles None and NaN values
   - Ensures stability in batch pipelines

Function:
---------
clean_github_issue(text: str) -> str

Input:
- Raw GitHub issue text (title, body, comments)

Output:
- Cleaned text preserving debugging-relevant information

Example:
--------
Input:
    "### Error\n```bash\nkubectl describe pod xyz\nOOMKilled\n```\n/sig node"

Output:
    "Error\nkubectl describe pod xyz\nOOMKilled"

Performance Considerations:
---------------------------
- Uses precompiled regex for high performance
- Suitable for large-scale datasets (thousands of issues)
- Efficient for batch processing in pipelines

Limitations:
------------
- Does not perform semantic understanding or summarization
- May still include long logs if not truncated
- URL removal may discard useful references in some cases

Future Improvements:
--------------------
- Smart detection of error/log sections
- Selective retention of important URLs (e.g., GitHub links)
- Stack trace parsing and structuring
- Adaptive cleaning modes (light vs aggressive)
- Integration with chunking and embedding pipelines

Usage:
------
Use this function after data extraction (e.g., GitHub API) and before:
- Chunking
- Embedding generation
- Vector database ingestion

Notes:
------
- Designed specifically for AI-powered debugging systems
- Preserving context is more important than aggressive cleaning
- Over-cleaning can degrade model performance in technical domains

"""

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
GITHUB_CMD_RE = re.compile(r"^/(sig|kind|priority|area|lifecycle)\b", re.IGNORECASE)

# ---------------------------
# 🔥 Helper: truncate code blocks (NOT remove)
# ---------------------------
def truncate_code_blocks(text, max_len=500):
    def replacer(match):
        block = match.group(0)
        return block[:max_len] + "..." if len(block) > max_len else block
    return CODE_BLOCK_RE.sub(replacer, text)

# ---------------------------
# 🔥 Main cleaning function
# ---------------------------
def clean_github_issue(text):
    # ---------------------------
    # Handle nulls safely
    # ---------------------------
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return ""

    text = str(text)

    # ---------------------------
    # Preserve but limit code blocks
    # ---------------------------
    text = truncate_code_blocks(text)

    # ---------------------------
    # Remove HTML noise
    # ---------------------------
    text = HTML_COMMENT_RE.sub(" ", text)
    text = html.unescape(text)
    text = HTML_TAG_RE.sub(" ", text)

    # ---------------------------
    # Remove URLs (optional)
    # ---------------------------
    text = URL_RE.sub(" ", text)

    # ---------------------------
    # Remove GitHub bot commands
    # ---------------------------
    lines = text.split("\n")
    lines = [line for line in lines if not GITHUB_CMD_RE.match(line.strip())]
    text = "\n".join(lines)

    # ---------------------------
    # Keep inline code content
    # ---------------------------
    text = INLINE_CODE_RE.sub(r"\1", text)

    # ---------------------------
    # Remove markdown symbols
    # ---------------------------
    text = MARKDOWN_RE.sub(" ", text)

    # ---------------------------
    # Normalize whitespace (preserve structure)
    # ---------------------------
    text = re.sub(r"[ \t]+", " ", text)     # collapse spaces
    text = re.sub(r"\n{3,}", "\n\n", text)  # limit newlines
    text = text.strip()

    return text



# import re
# import html
# import pandas as pd
# # ---------------------------
# # 🔥 Precompiled regex (FAST)
# # ---------------------------
# CODE_BLOCK_RE = re.compile(r"```.*?```", re.DOTALL)
# HTML_COMMENT_RE = re.compile(r"<!--.*?-->", re.DOTALL)
# HTML_TAG_RE = re.compile(r"<[^>]+>")
# URL_RE = re.compile(r"https?://\S+|www\.\S+")
# INLINE_CODE_RE = re.compile(r"`([^`]*)`")
# MARKDOWN_RE = re.compile(r"#+\s*|\*\*|__")
# WHITESPACE_RE = re.compile(r"\s+")

# # GitHub command matcher
# GITHUB_CMD_RE = re.compile(r"^/(sig|kind|priority|area|lifecycle)\b", re.IGNORECASE)

# def clean_github_issue(text):
#     # ---------------------------
#     # Handle nulls safely
#     # ---------------------------
#     if text is None or (isinstance(text, float) and pd.isna(text)):
#         return ""

#     text = str(text)

#     # ---------------------------
#     # Remove heavy noise first
#     # ---------------------------
#     text = CODE_BLOCK_RE.sub(" ", text)
#     text = HTML_COMMENT_RE.sub(" ", text)

#     # ---------------------------
#     # Normalize HTML
#     # ---------------------------
#     text = html.unescape(text)
#     text = HTML_TAG_RE.sub(" ", text)

#     # ---------------------------
#     # Remove URLs
#     # ---------------------------
#     text = URL_RE.sub(" ", text)

#     # ---------------------------
#     # Remove GitHub bot lines
#     # ---------------------------
#     lines = text.split("\n")
#     lines = [line for line in lines if not GITHUB_CMD_RE.match(line.strip())]
#     text = "\n".join(lines)

#     # ---------------------------
#     # Keep inline code content
#     # ---------------------------
#     text = INLINE_CODE_RE.sub(r"\1", text)

#     # ---------------------------
#     # Remove basic markdown
#     # ---------------------------
#     text = MARKDOWN_RE.sub(" ", text)

#     # ---------------------------
#     # Normalize whitespace (FINAL)
#     # ---------------------------
#     text = WHITESPACE_RE.sub(" ", text).strip()

#     return text