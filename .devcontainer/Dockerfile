# Lightweight dev container for GitHub Codespaces: the Alpine minirootfs image plus what the labs need.
# No lab images are pulled or started when the codespace is created; trainees start each lab's stack
# themselves with `docker compose ... up -d --wait` (see README, "Per-lab stack").
FROM alpine:3.24

# docker, docker-cli-compose, iptables: Docker engine and Compose, run inside the container (Docker-in-Docker)
# uv: installs the pinned Python and the locked host dependencies (`uv sync --locked`)
# build tools, librdkafka-dev: confluent-kafka has no musl wheel, so uv builds it against Alpine's librdkafka.
#   clang + compiler-rt, because uv's Python was built with clang and passes clang-only flags (--rtlib=compiler-rt);
#   binutils for the linker, musl-dev for the C headers
# bash, git, curl, libstdc++: the shell the labs use, and what the VS Code server needs on Alpine
RUN apk add --no-cache \
        bash git curl ca-certificates libstdc++ \
        docker docker-cli-compose iptables ip6tables \
        uv clang compiler-rt binutils musl-dev librdkafka-dev
ENV CC=clang

# dind: Moby's Docker-in-Docker wrapper (cgroup and mount setup), pinned to moby commit 8d9e350
COPY dind start-docker.sh /usr/local/bin/

ENTRYPOINT ["/usr/local/bin/start-docker.sh"]
CMD ["sleep", "infinity"]
