"""
Kubernetes Repository Code Extractor → JSONL Dataset Builder

Overview:
---------
This script recursively scans the local 'kubernetes' repository, extracts the
contents of selected file types, and stores them in a structured JSON Lines
(.jsonl) format. Each line in the output file represents a single file from
the repository, including its relative path, raw content, and file type.

Purpose:
--------
The primary goal of this script is to convert a large codebase into a structured,
machine-readable dataset that can be used for downstream AI/ML tasks such as:

- Retrieval-Augmented Generation (RAG)
- Code search and semantic retrieval
- LLM-based debugging and reasoning
- Static analysis and code understanding
- Building AI assistants for Kubernetes troubleshooting

Key Features:
-------------
1. Recursive Directory Traversal:
   - Walks through all subdirectories of the 'kubernetes' repository using os.walk.

2. File Type Filtering:
   - Processes only relevant file types:
     {".py", ".go", ".md", ".yaml", ".yml", ".json"}
   - Skips binaries and irrelevant files to reduce noise.

3. Parallel File Processing:
   - Uses ThreadPoolExecutor to read multiple files concurrently.
   - Improves performance significantly for large repositories.

4. Robust File Reading:
   - Opens files with UTF-8 encoding and ignores decoding errors.
   - Handles exceptions gracefully and skips problematic files.

5. Structured Output:
   - Each file is converted into a JSON object with:
        {
            "file": relative path from repo root,
            "content": full file content,
            "file_type": file extension
        }

6. JSONL Format:
   - Writes one JSON object per line to the output file.
   - Efficient for large datasets and streaming-based processing.

Output:
-------
- Output file path: /work/data/code.jsonl
- Format: JSON Lines (each line = one file)
- Suitable for direct ingestion into vector databases or ML pipelines.

Example Output Entry:
---------------------
{
    "file": "pkg/kubelet/kubelet.go",
    "content": "... full file content ...",
    "file_type": ".go"
}

Performance Considerations:
---------------------------
- Uses (CPU cores - 1) threads for parallelism.
- May consume significant memory for very large files.
- Output file size can grow large depending on repository size.

Limitations:
------------
- Stores entire file content without chunking (not optimal for LLM context limits).
- No semantic parsing (functions/classes not extracted separately).
- No embedding or indexing (raw dataset only).

Future Improvements:
--------------------
- Implement file chunking (e.g., 200–500 lines per chunk)
- Add metadata extraction (functions, classes, imports)
- Generate embeddings for semantic search
- Integrate with vector databases (FAISS, Chroma)
- Add filtering for extremely large files

Usage:
------
Ensure the 'kubernetes' repository is present in the working directory.
Run the script to generate the dataset:

    python script_name.py

This will create the output JSONL file at the specified path.

"""
import os
import json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

repo = 'kubernetes'
output_path = '/work/data/code.jsonl'
allowed_ext = {".py", ".go", ".md", ".yaml", ".yml", ".json"}

def return_content(file_path, ext):
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            file_content = f.read()
        data_point = {
            'file': os.path.relpath(file_path, start=repo),
            'content': file_content,
            'file_type': ext
        }
        return data_point
    except Exception as e:
        print(f"Failed to read {file_path}: {e}")
        return None

def extract_and_write():
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with ThreadPoolExecutor(max_workers=os.cpu_count() - 1) as executor, \
            open(output_path, 'w', encoding='utf-8') as f:

        futures = []

        for root_path, _, files in os.walk(repo):
            for file in files:
                ext = Path(file).suffix.lower()
                if ext not in allowed_ext:
                    continue

                file_path = os.path.join(root_path, file)
                futures.append(executor.submit(return_content, file_path, ext))

        for future in as_completed(futures):
            result = future.result()
            if result:
                f.write(json.dumps(result) + '\n')

if __name__ == "__main__":
    extract_and_write()