FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN go build -o crond ./cmd/crond

FROM alpine:3.20
WORKDIR /app
COPY --from=builder /app/crond .

ENV BIND_ADDR=:8001
ENV PEERS=localhost:8001
ENV ME=0
ENV API_ADDR=:8080

EXPOSE 8080
EXPOSE 8001

ENTRYPOINT ["./crond"]
