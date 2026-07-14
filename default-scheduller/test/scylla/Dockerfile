FROM golang:1.25-alpine AS builder
RUN apk add --no-cache git
RUN go install go.k6.io/xk6/cmd/xk6@latest
# Chỉ định rõ phiên bản k6 tương thích với xk6-cassandra (v0.50.0)
RUN xk6 build v0.50.0 --with github.com/grafana/xk6-cassandra
RUN mv k6 /usr/local/bin/k6

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY --from=builder /usr/local/bin/k6 /usr/bin/k6
ENTRYPOINT ["k6"]
