from pathlib import Path
import toml

PROJECT_ROOT = Path(__file__).parent.parent

VERSION_PATH = PROJECT_ROOT / "VERSION"
VERSION = toml.load(VERSION_PATH)["__version__"]

pyproject_path = PROJECT_ROOT / "pyproject.toml"
data = toml.load(pyproject_path)

packages_to_pin = {"summer-modules-core"}

new_deps = []
for dep in data["project"]["dependencies"]:
    if any(dep.startswith(pkg) for pkg in packages_to_pin):
        pkg_name = dep.split("==")[0]
        new_deps.append(f"{pkg_name}=={VERSION}")
    else:
        new_deps.append(dep)

data["project"]["dependencies"] = new_deps
pyproject_path.write_text(toml.dumps(data))
