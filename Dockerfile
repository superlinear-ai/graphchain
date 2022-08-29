# syntax=docker/dockerfile:1
FROM python:3.8-slim AS base

# Configure Python to print tracebacks on crash [1], and to not buffer stdout and stderr [2].
# [1] https://docs.python.org/3/using/cmdline.html#envvar-PYTHONFAULTHANDLER
# [2] https://docs.python.org/3/using/cmdline.html#envvar-PYTHONUNBUFFERED
ENV PYTHONFAULTHANDLER 1
ENV PYTHONUNBUFFERED 1

# Install Poetry.
ENV POETRY_VERSION 1.1.13
RUN --mount=type=cache,target=/root/.cache/ \
    pip install poetry==$POETRY_VERSION

# Create and activate a virtual environment.
RUN python -m venv /opt/app-env
ENV PATH /opt/app-env/bin:$PATH
ENV VIRTUAL_ENV /opt/app-env

# Install compilers that may be required for certain packages or platforms.
RUN rm /etc/apt/apt.conf.d/docker-clean
RUN --mount=type=cache,target=/var/cache/apt/ \
    --mount=type=cache,target=/var/lib/apt/ \
    apt-get update && \
    apt-get install --no-install-recommends --yes build-essential

# Set the working directory.
WORKDIR /app/

# Install the run time Python environment.
COPY poetry.lock* pyproject.toml /app/
RUN --mount=type=cache,target=/root/.cache/ \
    mkdir -p src/graphchain/ && touch src/graphchain/__init__.py && touch README.md && \
    poetry install --no-dev --no-interaction

# Create a non-root user.
ARG UID=1000
ARG GID=$UID
RUN groupadd --gid $GID app && \
    useradd --create-home --gid $GID --uid $UID app

FROM base as ci

# Install git so we can run pre-commit.
RUN --mount=type=cache,target=/var/cache/apt/ \
    --mount=type=cache,target=/var/lib/apt/ \
    apt-get update && \
    apt-get install --no-install-recommends --yes git

# Install the development Python environment.
RUN --mount=type=cache,target=/root/.cache/ \
    poetry install --no-interaction

# Give the non-root user ownership and switch to the non-root user.
RUN chown --recursive app /app/ /opt/
USER app

FROM base as dev

# Install development tools: compilers, curl, git, gpg, ssh, starship, sudo, vim, and zsh.
RUN --mount=type=cache,target=/var/cache/apt/ \
    --mount=type=cache,target=/var/lib/apt/ \
    apt-get update && \
    apt-get install --no-install-recommends --yes build-essential curl git gnupg ssh sudo vim zsh zsh-antigen && \
    sh -c "$(curl -fsSL https://starship.rs/install.sh)" -- "--yes" && \
    usermod --shell /usr/bin/zsh app

# Install the development Python environment.
RUN --mount=type=cache,target=/root/.cache/ \
    poetry install --no-interaction

# Persist output generated during docker build so that we can restore it in the dev container.
COPY .pre-commit-config.yaml /app/
RUN mkdir -p /opt/build/poetry/ && cp poetry.lock /opt/build/poetry/ && \
    git init && pre-commit install --install-hooks && \
    mkdir -p /opt/build/git/ && cp .git/hooks/commit-msg .git/hooks/pre-commit /opt/build/git/

# Give the non-root user ownership and switch to the non-root user.
RUN chown --recursive app /app/ /opt/ && \
    echo 'app ALL=(root) NOPASSWD:ALL' > /etc/sudoers.d/app && \
    chmod 0440 /etc/sudoers.d/app
USER app

# Configure the non-root user's shell.
RUN echo 'source /usr/share/zsh-antigen/antigen.zsh' >> ~/.zshrc && \
    echo 'antigen bundle zsh-users/zsh-syntax-highlighting' >> ~/.zshrc && \
    echo 'antigen bundle zsh-users/zsh-autosuggestions' >> ~/.zshrc && \
    echo 'antigen apply' >> ~/.zshrc && \
    echo 'eval "$(starship init zsh)"' >> ~/.zshrc && \
    echo 'HISTFILE=~/.zsh_history' >> ~/.zshrc && \
    zsh -c 'source ~/.zshrc'
