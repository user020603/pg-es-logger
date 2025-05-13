# Build Consumer Service
FROM golang:1.24.1 as builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
WORKDIR /app/cmd/consumer
RUN CGO_ENABLED=0 GOOS=linux go build -o /consumer_main

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /consumer_main .
CMD ["./consumer_main"]