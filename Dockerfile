FROM --platform=$BUILDPLATFORM ghcr.io/grafana/grafana-build-tools:v1.10.0@sha256:3c9d1ee0bceed38ad4aa9ca86bbad8f6b2a903b7647670f1858b784189ae5069 AS buildtools
WORKDIR /kube-node-labeler

COPY . .

ARG TARGETOS
ARG TARGETARCH

# Build with CGO_ENABLED=0 as grafana-build-tools is debian-based.
RUN --mount=type=cache,target=/root/.cache/go-build \
  --mount=type=cache,target=/root/go/pkg \
  CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /usr/local/bin/kube-node-labeler .

FROM alpine:3.22.8@sha256:e1011cfb1c3f0d0a3a06d5371ef866b5713e035a866d84a419e0e3d73ca0dfd8

COPY --from=buildtools /usr/local/bin/kube-node-labeler /usr/local/bin

ENTRYPOINT [ "/usr/local/bin/kube-node-labeler" ]
