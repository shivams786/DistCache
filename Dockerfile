FROM golang:1.22-alpine AS build

WORKDIR /src
COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/distcache ./cmd/server

FROM alpine:3.20

RUN addgroup -S distcache && adduser -S distcache -G distcache
USER distcache
WORKDIR /app
COPY --from=build /out/distcache /app/distcache

EXPOSE 8080 9090
ENTRYPOINT ["/app/distcache"]
