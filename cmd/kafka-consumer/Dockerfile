# build stage
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Copy go.mod and go.sum to download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the entire project
COPY . .

# Build the kafka-consumer
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/kafka-consumer ./cmd/kafka-consumer

# final stage
FROM alpine:latest

WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/kafka-consumer .
COPY ./cmd/kafka-consumer/config.yml config.yml
# The config file will be mounted as a volume.
# The command will be provided in docker-compose.
ENTRYPOINT ["./kafka-consumer", "config.yml"]

