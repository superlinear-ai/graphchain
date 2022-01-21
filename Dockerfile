# syntax=docker/dockerfile:experimental

FROM python:3.8-slim AS dev

# Install development tools: compilers, curl, fish, git, ssh, and starship.
RUN apt-get update && \
    apt-get install --no-install-recommends --yes build-essential curl git fish ssh && \
    chsh --shell /usr/bin/fish && \
    sh -c "$(curl -fsSL https://starship.rs/install.sh)" -- "--yes" && \
    mkdir -p ~/.config/fish/completions/ && \
    echo "set fish_greeting" >> ~/.config/fish/config.fish && \
    echo "starship init fish | source" >> ~/.config/fish/config.fish && \
    rm -rf /var/lib/apt/lists/*

# Configure Python to print tracebacks on crash [1], and to not buffer stdout and stderr [2].
# [1] https://docs.python.org/3/using/cmdline.html#envvar-PYTHONFAULTHANDLER
# [2] https://docs.python.org/3/using/cmdline.html#envvar-PYTHONUNBUFFERED
ENV PYTHONFAULTHANDLER 1
ENV PYTHONUNBUFFERED 1

# Set the working directory.
WORKDIR /app/

# Install base development environment with Poetry and Poe the Poet.
ENV PATH /root/.local/bin/:$PATH
RUN --mount=type=cache,target=/root/.cache \
    pip install --no-input --upgrade pip poethepoet && \
    curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/install-poetry.py | python - && \
    poetry config virtualenvs.create false && \
    poetry completions fish > ~/.config/fish/completions/poetry.fish && \
    poe _fish_completion > ~/.config/fish/completions/poe.fish

# Let Poe the Poet know it doesn't need to activate the Python environment.
ENV POETRY_ACTIVE 1

# Enable Poetry to publish to PyPI [1].
# [1] https://pythonspeed.com/articles/build-secrets-docker-compose/
ARG POETRY_PYPI_TOKEN_PYPI
ENV POETRY_PYPI_TOKEN_PYPI $POETRY_PYPI_TOKEN_PYPI

FROM dev as ci

# Install the Python environment.
# TODO: Replace `--no-dev` with `--without test` when Poetry 1.2.0 is released.
COPY poetry.lock pyproject.toml /app/
RUN --mount=type=cache,target=/root/.cache \
    mkdir -p src/graphchain/ && touch src/graphchain/__init__.py && touch README.md && \
    poetry install --no-dev --no-interaction
