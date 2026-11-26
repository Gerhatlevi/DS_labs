FROM golang:1.27.3-alpine AS builder

# setup a main app working directory
WORKDIR /app

# install dependencies for building
RUN apk add build-base

# copy over the main files into the current dir
COPY . ./

# build the binaries
WORKDIR /app/main
RUN CGO_ENABLED=1 go build -race -o mrcoordinator mrcoordinator.go
RUN CGO_ENABLED=1 go build -race -o mrworker mrworker.go
RUN CGO_ENABLED=1 go build -race -buildmode=plugin ../mrapps/wc.go

# only copy over the files we need to reduce image size
FROM alpine:latest

WORKDIR /app
COPY --from=builder /app/main/mrworker /app/main/mrcoordinator /app/main/wc.so /app/main/pg-* .
