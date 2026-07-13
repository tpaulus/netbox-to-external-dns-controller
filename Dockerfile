FROM golang:1.26 AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/netbox-to-external-dns-controller ./...

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/netbox-to-external-dns-controller /netbox-to-external-dns-controller

ENTRYPOINT ["/netbox-to-external-dns-controller"]
