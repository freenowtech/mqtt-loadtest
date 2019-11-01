ARG GOLANG_VERSION
FROM golang:${GOLANG_VERSION} AS build
WORKDIR /go/src/github.com/freenowtech/mqtt-loadtest/
COPY . .
RUN go build -o mqtt-loadtest.linux-amd64

FROM gcr.io/distroless/base
USER nobody
COPY --from=build /go/src/github.com/freenowtech/mqtt-loadtest/mqtt-loadtest.linux-amd64 /app
ENTRYPOINT ["/app"]
