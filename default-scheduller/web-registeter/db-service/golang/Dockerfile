# STAGE 1: Build the binary
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copy everything and build
COPY . .

# Download dependencies and build
RUN go mod download && \
    CGO_ENABLED=0 GOOS=linux go build -o main .

# STAGE 2: Minimal runtime image
FROM alpine:latest

RUN adduser -D appuser
USER appuser

WORKDIR /app
COPY --from=builder /app/main .

EXPOSE 3000
CMD ["./main"]
