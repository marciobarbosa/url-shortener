FROM golang:1.21-alpine AS build

WORKDIR /

# Get dependencies
COPY go.mod go.sum ./
#COPY go.mod ./
RUN go mod download

# Copy source code and libraries
COPY ./cmd /cmd
COPY ./pkg /pkg
COPY ./internal /internal

# Build
RUN go build -o /build/bin/kv-server /cmd/server/main.go

# Create a lean image
FROM scratch

WORKDIR /

# Copy only the binary
COPY --from=build /build/bin/kv-server /bin/kv-server

ENTRYPOINT ["/bin/kv-server"]
