FROM golang:1.27.3-alpine AS builder

# setup a main app working directory
WORKDIR /app

# copy over the main files into the current dir
COPY go.mod *.go ./

# build the binary
RUN go build -o http_server .

# only copy over the files we need to reduce image size
FROM alpine:latest

WORKDIR /app
COPY --from=builder /app/http_server .

# document that the default exposed port will be 5555
EXPOSE 5555

# start the server at port 5555
ENTRYPOINT ["./http_server", "5555"]
