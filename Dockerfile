# syntax=docker/dockerfile:experimental
FROM python:3.8-slim AS base

# Configure Python to print tracebacks on crash [1], and to not buffer stdout and stderr [2].
# [1] https://docs.python.org/3/using/cmdline.html#envvar-PYTHONFAULTHANDLER
# [2] https://docs.python.org/3/using/cmdline.html#envvar-PYTHONUNBUFFERED
ENV PYTHONFAULTHANDLER 1
ENV PYTHONUNBUFFERED 1

# Install Poetry.
ENV POETRY_VERSION 1.1.13
RUN --mount=type=cache,id=poetry,target=/root/.cache/ pip install poetry==$POETRY_VERSION

# Create and activate a virtual environment.
RUN python -m venv /opt/app-env
ENV PATH /opt/app-env/bin:$PATH
ENV VIRTUAL_ENV /opt/app-env

# Set the working directory.
WORKDIR /app/

FROM base as dev

# Install development tools: compilers, curl, git, gpg, ssh, starship, vim, and zsh.
RUN rm /etc/apt/apt.conf.d/docker-clean
RUN --mount=type=cache,id=apt-cache,target=/var/cache/apt/ \
    --mount=type=cache,id=apt-lib,target=/var/lib/apt/ \
    apt-get update && \
    apt-get install --no-install-recommends --yes build-essential curl git gnupg ssh vim zsh zsh-antigen && \
    chsh --shell /usr/bin/zsh && \
    sh -c "$(curl -fsSL https://starship.rs/install.sh)" -- "--yes" && \
    echo 'source /usr/share/zsh-antigen/antigen.zsh' >> ~/.zshrc && \
    echo 'antigen bundle zsh-users/zsh-autosuggestions' >> ~/.zshrc && \
    echo 'antigen apply' >> ~/.zshrc && \
    echo 'eval "$(starship init zsh)"' >> ~/.zshrc && \
    zsh -c 'source ~/.zshrc'

# Install the development Python environment.
COPY .pre-commit-config.yaml poetry.lock* pyproject.toml /app/
RUN --mount=type=cache,id=poetry,target=/root/.cache/ \
    mkdir -p src/graphchain/ && touch src/graphchain/__init__.py && touch README.md && \
    poetry install --no-interaction && \
    mkdir -p /var/lib/poetry/ && cp poetry.lock /var/lib/poetry/ && \
    git init && pre-commit install --install-hooks && \
    mkdir -p /var/lib/git/ && cp .git/hooks/commit-msg .git/hooks/pre-commit /var/lib/git/

FROM base as ci

# Install the run time Python environment.
# TODO: Replace `--no-dev` with `--without test` when Poetry 1.2.0 is released.
COPY poetry.lock pyproject.toml /app/
RUN --mount=type=cache,id=poetry,target=/root/.cache/ \
    mkdir -p src/graphchain/ && touch src/graphchain/__init__.py && touch README.md && \
    poetry install --no-dev --no-interaction
