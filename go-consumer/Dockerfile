FROM golang:1.24-alpine3.22 AS builder

RUN apk add --no-cache gcc musl-dev

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=1 go build -o /go-consumer main.go

FROM alpine:3.18

RUN apk add --no-cache libpq ca-certificates

COPY --from=builder /go-consumer /consumer

CMD ["/consumer"]