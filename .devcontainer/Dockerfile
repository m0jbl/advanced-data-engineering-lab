# GitHub Codespaces: one small Alpine container, no Docker inside it.
# The lab services (Kafka, Spark, MinIO, ...) run as processes in this container; scripts/codespace.sh
# installs each stack the first time a lab starts it. On a laptop the same services run in Docker Compose.
#
# Alpine 3.22: its Python 3.12, pandas 2.2.3, numpy 2.2 and pyarrow 19.0.1 match the laptop Spark image.
FROM alpine:3.22

# bash, git, curl, procps, libstdc++: the shell and tools the labs use, and what the VS Code server needs
# python3, python3-dev, build-base, librdkafka-dev: the host Python; confluent-kafka has no musl wheel,
#   so uv builds it against Alpine's librdkafka (2.10, hence confluent-kafka 2.10.0 in pyproject.toml)
RUN apk add --no-cache \
        bash git curl ca-certificates procps libstdc++ \
        python3 python3-dev build-base librdkafka-dev

# uv from its official image (Alpine 3.22's uv is too old for uv.lock); it uses the system Python 3.12
COPY --from=ghcr.io/astral-sh/uv:0.11.19 /uv /uvx /usr/local/bin/
ENV UV_PYTHON_DOWNLOADS=never UV_PYTHON_PREFERENCE=only-system
