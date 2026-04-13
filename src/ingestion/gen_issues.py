"""
GitHub Issues Extractor (GraphQL) → Kubernetes Dataset Builder

Overview:
---------
This script fetches issues from the Kubernetes GitHub repository using the
GitHub GraphQL API and stores them in a structured JSON Lines (.jsonl) format.
It supports incremental data collection using cursor-based pagination, allowing
it to resume from the last fetched position without duplicating data.

Purpose:
--------
The goal of this script is to build a high-quality dataset of real-world
Kubernetes issues, which can be used for:

- Training or fine-tuning LLMs for Kubernetes troubleshooting
- Building Retrieval-Augmented Generation (RAG) systems
- Creating debugging assistants for DevOps/Kubernetes
- Analyzing common failure patterns and root causes
- Constructing evaluation benchmarks for AI systems

Key Features:
-------------
1. GraphQL-Based Fetching:
   - Uses GitHub's GraphQL API for efficient and structured data retrieval.
   - Fetches issues in batches of 50 (configurable via query).

2. Incremental Pagination (Cursor-Based):
   - Maintains a cursor (saved in 'cursor.txt') to track progress.
   - Allows resuming from the last fetched issue without restarting.

3. Rich Issue Data Extraction:
   For each issue, extracts:
   - ID, number, title, and description (body)
   - Creation timestamp and state
   - Labels (important for categorization)
   - Milestone (if available)
   - Comments (up to 20 per issue)

4. JSONL Output Format:
   - Stores each issue as a separate JSON object (one per line).
   - Efficient for large-scale data processing and streaming pipelines.

5. Environment-Based Authentication:
   - Loads GitHub API token securely from a `.env` file.
   - Prevents hardcoding sensitive credentials.

6. Rate Limit Handling:
   - Adds a small delay (0.5s) between requests to avoid API throttling.

7. Fault Tolerance:
   - Stops execution if API errors occur.
   - Ensures partial progress is saved (append mode + cursor tracking).

Output:
-------
- File: data/raw/TEINGDATA.jsonl
- Format: JSON Lines
- Each line contains:
    {
        "id": "...",
        "number": 12345,
        "title": "...",
        "body": "...",
        "created_at": "...",
        "comments": [...],
        "labels": [...],
        "milestone": "..."
    }

Cursor Management:
------------------
- Cursor is stored in: cursor.txt
- Used to resume fetching from last processed issue
- Prevents duplicate downloads and enables incremental updates

Performance Considerations:
---------------------------
- Fetches 50 issues per request (GitHub limit)
- Includes up to 20 comments per issue
- Adds delay to respect API rate limits
- Output file grows linearly with number of issues

Limitations:
------------
- Does not handle deleted/edited issues after initial fetch
- Only fetches first 20 comments per issue
- No retry logic for transient network failures
- No data cleaning or preprocessing applied

Future Improvements:
--------------------
- Add retry logic with exponential backoff
- Increase robustness for API failures
- Implement deduplication checks
- Add preprocessing/cleaning pipeline (remove noise, trim text)
- Integrate chunking for LLM-ready data
- Push data directly into vector database (FAISS/Chroma)
- Support multi-repository ingestion

Usage:
------
1. Create a `.env` file in the project root with:
       GITHUB_TOKEN=your_token_here

2. Ensure output directories exist or will be created automatically.

3. Run the script:
       python script_name.py

4. The script will:
   - Resume from last cursor (if exists)
   - Fetch issues incrementally
   - Append results to JSONL file

Notes:
------
- Designed for building real-world datasets for AI systems.
- Particularly useful for Kubernetes debugging and issue resolution use cases.

"""
import requests
import json
import time
import os
from dotenv import load_dotenv
from pathlib import Path

env_path = Path(__file__).resolve().parent.parent.parent / ".env"
load_dotenv(dotenv_path=env_path)
github_token = os.getenv("GITHUB_TOKEN")

url = "https://api.github.com/graphql"

OUTPUT_FOLDER_PATH = Path(__file__).resolve().parent.parent.parent /"data/raw"
CURSOR_PATH = Path(__file__).resolve().parent/"cursor.txt"

headers = {
    "Authorization": f"Bearer " + github_token
}

query = """
query ($cursor: String) {
  repository(owner: "kubernetes", name: "kubernetes") {
    issues(first: 50, after: $cursor, orderBy: {field: CREATED_AT, direction: ASC}) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        id
        number
        title
        body
        createdAt
        state

        # LABELS (MOST IMPORTANT)
        labels(first: 20) {
          nodes {
            name
          }
        }

        # MILESTONE
        milestone {
          title
        }

        # COMMENTS
        comments(first: 20) {
          nodes {
            body
          }
        }
      }
    }
  }
}
"""

def fetch_issues_graphql():

    cursor = None
    if os.path.exists(CURSOR_PATH):
        with open(CURSOR_PATH, "r") as f:
            cursor = f.read() or None

    with open(OUTPUT_FOLDER_PATH /'TEINGDATA.jsonl', "a") as f:

        while True:
            variables = {"cursor": cursor}

            response = requests.post(
                url,
                json={"query": query, "variables": variables},
                headers=headers
            )

            data = response.json()

            # 🔴 error handling
            if "errors" in data:
                print("Error:", data["errors"])
                break

            issues_data = data["data"]["repository"]["issues"]

            nodes = issues_data["nodes"]

            print(f"Fetched {len(nodes)} issues")

            for issue in nodes:
                row = {
                    "id": issue["id"],
                    "number": issue["number"],
                    "title": issue["title"],
                    "body": issue["body"],
                    "created_at": issue["createdAt"],
                    "comments": [c["body"] for c in issue["comments"]["nodes"]],
                    "labels":[c["name"] for c in issue["labels"]["nodes"]],
                    "milestone":issue.get("milestone", {}).get("title") if issue.get("milestone") else None,
                }

                f.write(json.dumps(row) + "\n")

            f.flush()

            # pagination
            if not issues_data["pageInfo"]["hasNextPage"]:
                print("✅ Done")
                break

            cursor = issues_data["pageInfo"]["endCursor"]
            
            with open("cursor.txt", "w") as cursor_f:
                cursor_f.write(cursor if cursor else "")

            time.sleep(0.5)  # rate limit safety

fetch_issues_graphql()