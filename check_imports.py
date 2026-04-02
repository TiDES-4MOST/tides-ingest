import ast
import os
import sys

def get_imports(file_path):
    with open(file_path, 'r') as f:
        try:
            tree = ast.parse(f.read(), filename=file_path)
        except SyntaxError as e:
            print(f"Error parsing {file_path}: {e}")
            return set()

    imports = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.add(alias.name.split('.')[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imports.add(node.module.split('.')[0])
    return imports

files = [
    "opr4-test-pipeline.py",
    "opr4_flows/opr4_controller.py",
    "opr4_flows/opr4_ztf.py",
    "opr4_flows/submit_transients.py",
    "opr4_flows/tidesCom.py"
]

all_imports = set()
for file_path in files:
    if os.path.exists(file_path):
        all_imports.update(get_imports(file_path))
    else:
        print(f"File not found: {file_path}")

print("Detected Imports:")
for imp in sorted(all_imports):
    print(imp)
