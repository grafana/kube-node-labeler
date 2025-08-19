FROM --platform=$BUILDPLATFORM ghcr.io/grafana/grafana-build-tools:v1.14.0@sha256:976d7ceebf570f078ef136580d6abb1bc68d921ac8c716ed3528fcccb16a7a6a AS buildtools
WORKDIR /kube-node-labeler

COPY . .

ARG TARGETOS
ARG TARGETARCH

# Build with CGO_ENABLED=0 as grafana-build-tools is debian-based.
RUN --mount=type=cache,target=/root/.cache/go-build \
  --mount=type=cache,target=/root/go/pkg \
  CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /usr/local/bin/kube-node-labeler .

FROM alpine:3.22.1@sha256:4bcff63911fcb4448bd4fdacec207030997caf25e9bea4045fa6c8c44de311d1

COPY --from=buildtools /usr/local/bin/kube-node-labeler /usr/local/bin

ENTRYPOINT [ "/usr/local/bin/kube-node-labeler" ]
