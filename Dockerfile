# ==========================================
# Stage 1: The Builder (Compiles the code)
# ==========================================
FROM golang:1.26-alpine AS builder

# Install git, curl, litecli, AND the required CGO dependencies (gcc, musl-dev)
RUN apk add --no-cache git curl litecli gcc musl-dev

WORKDIR /app

# 1. Cache Dependencies
# We copy ONLY the mod files first. Docker caches this layer. 
# If go.mod hasn't changed, Docker skips 'go mod download'.
COPY go.mod go.sum ./
RUN go mod download

# 2. Copy Source & Build
COPY . .

# Build with CGO enabled and statically link the C libraries
RUN CGO_ENABLED=1 GOOS=linux go build -a -ldflags="-w -s -extldflags '-static'" -o main .

# ==========================================
# Stage 2: The Runner (Final small image)
# ==========================================
FROM alpine:latest

WORKDIR /app

# Copy the statically linked binary from the builder stage
COPY --from=builder /app/main .

# Expose the application port
EXPOSE 3000

# Run the binary
CMD ["./main"]
