# ==========================================
# Stage 1: The Builder (Compiles the code)
# ==========================================
FROM golang:1.25-alpine AS builder

# Install git. (Alpine images are minimal and might miss git required for some deps)
RUN apk add --no-cache git
RUN apk add curl

WORKDIR /app

# 1. Cache Dependencies
# We copy ONLY the mod files first. Docker caches this layer. 
# If go.mod hasn't changed, Docker skips 'go mod download'.
COPY go.mod go.sum ./
RUN go mod download

# 2. Copy Source & Build
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o main .

# ==========================================
# Stage 2: The Runner (Final small image)
# ==========================================
FROM alpine:latest


WORKDIR /app
# Copy the binary from the builder stage
COPY --from=builder /app/main .



# Expose the application port
EXPOSE 3000

# Run the binary
CMD ["./main"]
