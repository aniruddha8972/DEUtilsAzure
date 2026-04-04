# Installing de_utils

## The one command you need

```bash
cd de_utils_project
pip install -e .
```

That's it. After that, `import de_utils` works anywhere on your machine in the same Python environment.

---

## Step-by-step for VSCode

### 1. Unzip and open the folder

```bash
unzip de_utils_v2_complete.zip
cd de_utils_project
code .          # open in VSCode
```

### 2. Pick your Python interpreter in VSCode

Press `Ctrl+Shift+P` → **Python: Select Interpreter** → choose the interpreter you use for your project (venv, conda, system Python).

### 3. Install the package into that interpreter

Open the VSCode terminal (`Ctrl+`` `) and run:

```bash
# Core only (requires PySpark already installed)
pip install -e .

# With Azure ADLS SDK
pip install -e ".[azure]"

# With Azure Key Vault
pip install -e ".[keyvault]"

# With YAML config support
pip install -e ".[yaml]"

# Everything at once
pip install -e ".[all]"

# For running tests
pip install -e ".[dev]"
```

The `-e` flag means **editable install** — you can edit the source files and changes take effect immediately without reinstalling.

### 4. Verify it works

```python
# In VSCode terminal or any .py file
import de_utils
print(de_utils.__version__)   # 2.1.0

from de_utils import ADLSConfig, DataQualityChecker, Rule
print("✅ de_utils imported successfully")
```

---

## Using a virtual environment (recommended)

```bash
cd de_utils_project

# Create venv
python -m venv .venv

# Activate
source .venv/bin/activate          # macOS / Linux
.venv\Scripts\activate             # Windows

# Install
pip install -e ".[dev]"

# Select in VSCode:
# Ctrl+Shift+P → Python: Select Interpreter → ./.venv/bin/python
```

---

## Using conda

```bash
conda create -n de_utils python=3.10
conda activate de_utils
cd de_utils_project
pip install -e ".[dev]"
```

---

## Run the tests

```bash
pytest tests/ -v
```

---

## Why `pip install -e .` and not just copying files?

Python needs to know where to find the package. Without installing, it only looks in the current directory and standard library paths. Running `pip install -e .` registers the package location with Python so `import de_utils` works from any folder, notebook, or script in that environment.

If you can't or don't want to install, you can also add the project root to `PYTHONPATH`:

```bash
# macOS / Linux
export PYTHONPATH="/path/to/de_utils_project:$PYTHONPATH"

# Windows (PowerShell)
$env:PYTHONPATH = "C:\path\to\de_utils_project;$env:PYTHONPATH"

# Or in VSCode settings.json
{
    "terminal.integrated.env.osx": {
        "PYTHONPATH": "/path/to/de_utils_project"
    }
}
```

But `pip install -e .` is cleaner and the recommended approach.
