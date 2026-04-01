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

# query = """
# query ($cursor: String) {
#   repository(owner: "kubernetes", name: "kubernetes") {
#     issues(first: 50, after: $cursor, orderBy: {field: CREATED_AT, direction: ASC}) {
#       pageInfo {
#         hasNextPage
#         endCursor
#       }
#       nodes {
#         id
#         number
#         title
#         body
#         createdAt
#         comments(first: 20) {
#           nodes {
#             body
#           }
#         }
#       }
#     }
#   }
# }
# """

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