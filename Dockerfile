FROM --platform=$BUILDPLATFORM ghcr.io/grafana/grafana-build-tools:v1.12.1@sha256:29bdd25f292a23a79bc13902dce1546597b5dc0fb6727730f346dab0e336b26f AS buildtools
WORKDIR /kube-node-labeler

COPY . .

ARG TARGETOS
ARG TARGETARCH

# Build with CGO_ENABLED=0 as grafana-build-tools is debian-based.
RUN --mount=type=cache,target=/root/.cache/go-build \
  --mount=type=cache,target=/root/go/pkg \
  CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /usr/local/bin/kube-node-labeler ./cmd

FROM alpine:3.22.1@sha256:4bcff63911fcb4448bd4fdacec207030997caf25e9bea4045fa6c8c44de311d1

COPY --from=buildtools /usr/local/bin/kube-node-labeler /usr/local/bin

ENTRYPOINT [ "/usr/local/bin/kube-node-labeler" ]
