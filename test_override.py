import tomlkit

with open("pyproject.toml") as f:
    doc = tomlkit.load(f)

if "tool" in doc and "uv" in doc["tool"] and "sources" in doc["tool"]["uv"]:
    del doc["tool"]["uv"]["sources"]

if "tool" not in doc:
    doc["tool"] = {}
if "uv" not in doc["tool"]:
    doc["tool"]["uv"] = {}

doc["tool"]["uv"]["override-dependencies"] = [
    "orjson @ ./mocks/orjson",
    "pywin32 @ ./mocks/pywin32",
]

with open("pyproject.toml", "w") as f:
    tomlkit.dump(doc, f)
