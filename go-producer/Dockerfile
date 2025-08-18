FROM golang:1.24-alpine3.22 AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -o /go-producer main.go

FROM scratch

COPY --from=builder /go-producer /producer

CMD ["/producer"]