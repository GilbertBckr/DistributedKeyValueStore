# ==========================================
# Stage 1: The Builder (Compiles the code)
# ==========================================
FROM golang:1.25-alpine AS builder

# Install git. (Alpine images are minimal and might miss git required for some deps)
RUN apk add --no-cache git

WORKDIR /app

# 1. Cache Dependencies
# We copy ONLY the mod files first. Docker caches this layer. 
# If go.mod hasn't changed, Docker skips 'go mod download'.
COPY go.mod go.sum ./
RUN go mod download

# 2. Copy Source & Build
COPY . .

# Build the binary.
# -o main: name the output binary "main"
# CGO_ENABLED=0: Disables CGO for a static binary (Required for 'glebarez' sqlite)
# -ldflags="-w -s": Strips debug information to reduce binary size
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o main .

# ==========================================
# Stage 2: The Runner (Final small image)
# ==========================================
FROM alpine:latest

# Security: Install ca-certificates for HTTPS calls and setup a non-root user
RUN apk --no-cache add ca-certificates && \
    adduser -D -g '' appuser

WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/main .

# Create the data directory for SQLite and give ownership to the non-root user
RUN mkdir /app/data && chown appuser:appuser /app/data

# Switch to non-root user for security
USER appuser

# Expose the application port
EXPOSE 3000

# Run the binary
CMD ["./main"]
