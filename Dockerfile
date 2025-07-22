FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o main ./run && chmod +x main

FROM scratch

COPY --from=builder /app/main /

EXPOSE 6336

ENTRYPOINT ["/main", "server"]