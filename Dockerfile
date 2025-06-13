FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -ldflags="-s -w" -o main

FROM scratch

COPY --from=builder /app/main /

EXPOSE 8080
ENTRYPOINT ["/main", "server"]