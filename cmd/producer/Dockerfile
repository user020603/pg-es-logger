# Build Producer Service
FROM golang:1.24.1 as builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
WORKDIR /app/cmd/producer
RUN CGO_ENABLED=0 GOOS=linux go build -o /producer_main

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /producer_main .
CMD ["./producer_main"]