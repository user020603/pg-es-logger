FROM golang:1.24.1 as builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o sync-service ./cmd/main.go

FROM alpine:3.17

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

COPY --from=builder /app/sync-service .

# Create log directory
RUN mkdir -p /app/log

ENTRYPOINT ["/app/sync-service"]