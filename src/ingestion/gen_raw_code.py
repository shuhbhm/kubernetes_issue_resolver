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

    
#  import os
# import json
# from pathlib import Path
# from concurrent.futures import ThreadPoolExecutor,as_completed

# repo = 'kubernetes'
# output_path = 'data/code.jsonl'

# allowed_ext = {".py", ".go", ".md", ".yaml", ".yml", ".json"}

# def return_content(file_path, ext):
#     with open(file_path,'r', encoding='utf-8', errors='ignore') as f:
#         file_content = f.read()
#         data_point = {
#                 'file':file_path,
#                 'content':file_content,
#                 'file_type':ext
#             }
#     return data_point

# def extract_and_write():
#     with ThreadPoolExecutor(max_workers=os.cpu_count()-1) as executor, open(output_path, 'w', encoding='utf-8') as f:

#         futures = []

#         for root_path, _, files in os.walk(repo):
#             for file in files:
#                 ext = Path(file).suffix
#                 if ext not in allowed_ext:
#                     continue

#                 file_path = os.path.join(root_path, file)
#                 futures.append(executor.submit(return_content, file_path, ext))

#         for future in as_completed(futures):
#             result = future.result()
#             if result:
#                 f.write(json.dumps(result) + '\n')