# syntax=docker/dockerfile:1
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/likho-analytics ./cmd/likho-analytics

FROM alpine:3.24
RUN addgroup -S likho && adduser -S -u 10001 -G likho likho
COPY --from=build /out/likho-analytics /usr/local/bin/likho-analytics
USER likho
EXPOSE 4070 5070
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=5 \
    CMD ["wget", "-qO-", "http://127.0.0.1:4070/readyz"]
CMD ["likho-analytics"]
