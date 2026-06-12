# syntax=docker/dockerfile:1

# Shared multi-stage build for all three services. They live in one Go module
# and differ only by which cmd/ package is the entrypoint, so a single ARG
# selects the target instead of duplicating three near-identical Dockerfiles.
ARG SERVICE

FROM golang:1.25-alpine AS build
ARG SERVICE
WORKDIR /src
# Download modules in their own layer so source edits don't bust the cache.
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/service ./cmd/${SERVICE}

# Distroless static: no shell, no package manager, runs as non-root.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/service /service
USER nonroot:nonroot
ENTRYPOINT ["/service"]
