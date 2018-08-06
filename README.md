[![CircleCI](https://img.shields.io/circleci/token/39b1cfd1096f95ab3c6aeb839d86763ea2a261aa/project/radix-ai/graphchain/master.svg)](https://circleci.com/gh/radix-ai/graphchain/tree/master) [![License: GPL v3](https://img.shields.io/badge/license-GPL%20v3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

# Graphchain

## What is graphchain?

Graphchain is like joblib.Memory for dask graphs.

## Example usage

Checkout the `examples` folder for API usage examples.

## Editor setup

1.  Install [VSCode](https://code.visualstudio.com/) as your editor.
2.  Install the following VSCode extensions:
    - [Python](https://marketplace.visualstudio.com/items?itemName=ms-python.python) to add Python support.
3.  In VSCode, select `jobnet-env` as your Python environment from the blue bar.
4.  In VSCode, edit your _Workspace Settings_ [CMD+,] so that you have:

```json
"python.pythonPath": "/Users/laurent/miniconda3/envs/graphchain-env/bin/python",
// Automatically format code and imports on save.
"editor.formatOnPaste": true,
"editor.formatOnSave": true,
"python.formatting.provider": "autopep8",
"[python]": {
"editor.codeActionsOnSave": {
    "source.organizeImports": true
}
},
// Run linters in background while you develop.
"python.linting.pylintEnabled": false,
"python.linting.pydocstyleEnabled": true,
"python.linting.pydocstyleArgs": ["--convention=numpy"],
"python.linting.mypyEnabled": true,
"python.linting.mypyArgs": ["--ignore-missing-imports", "--strict"],
"python.linting.flake8Enabled": true,
"python.linting.flake8Args": ["--max-complexity=10"],
// Testing.
"python.unitTest.pyTestEnabled": true,
"python.unitTest.useExperimentalDebugger": true,
// Activate the conda environment, load the config environment variables, and load AWS credentials every time you open a terminal.
"terminal.integrated.shellArgs.osx": [
    // bash:
    "--init-file <(echo \"source activate graphchain-env\")"
    // fish:
    //"--init-command=conda activate graphchain-env"
]
```
