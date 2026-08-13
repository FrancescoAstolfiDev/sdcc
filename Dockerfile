FROM node:22-slim

RUN apt-get update && apt-get install -y \
    protobuf-compiler git curl make poppler-utils wget \
    ca-certificates gnupg lsb-release \
    && rm -rf /var/lib/apt/lists/*

# Installa Docker CLI + compose plugin (solo client, non il daemon)
RUN install -m 0755 -d /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc \
    && chmod a+r /etc/apt/keyrings/docker.asc \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian $(. /etc/os-release && echo $VERSION_CODENAME) stable" \
    > /etc/apt/sources.list.d/docker.list \
    && apt-get update \
    && apt-get install -y docker-ce-cli docker-compose-plugin \
    && rm -rf /var/lib/apt/lists/*

RUN wget -q https://go.dev/dl/go1.23.6.linux-amd64.tar.gz \
    && tar -C /usr/local -xzf go1.23.6.linux-amd64.tar.gz \
    && rm go1.23.6.linux-amd64.tar.gz

ENV PATH="${PATH}:/usr/local/go/bin:/home/node/go/bin"

RUN npm install -g @anthropic-ai/claude-code

# Permette all'utente node di usare il socket Docker montato
RUN groupadd -f docker && usermod -aG docker node

USER node
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest \
    && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

WORKDIR /workspace
CMD ["claude", "--dangerously-skip-permissions"]