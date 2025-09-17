# Build stage
FROM golang:1.21-alpine AS builder

# Install git and ca-certificates for fetching dependencies
RUN apk add --no-cache git ca-certificates tzdata

# Set working directory
WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Verify dependencies
RUN go mod verify

# Copy source code
COPY . .

# Build arguments for version info
ARG VERSION=dev
ARG COMMIT=unknown
ARG DATE=unknown

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-X 'main.version=${VERSION}' -X 'main.gitCommit=${COMMIT}' -X 'main.buildDate=${DATE}' -w -s" \
    -o ddb .

# Test the binary
RUN ./ddb version

# Runtime stage
FROM alpine:latest

# Install ca-certificates for HTTPS requests
RUN apk --no-cache add ca-certificates

# Create non-root user
RUN addgroup -g 1001 -S ddb && \
    adduser -u 1001 -S ddb -G ddb

# Set working directory
WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/ddb /usr/local/bin/ddb

# Copy sample data and configs (optional)
COPY --chown=ddb:ddb test/data/ /app/data/
COPY --chown=ddb:ddb docs/ /app/docs/

# Create directories for user data
RUN mkdir -p /app/input /app/output /app/configs && \
    chown -R ddb:ddb /app

# Switch to non-root user
USER ddb

# Expose no ports (CLI tool)
# EXPOSE 8080

# Set entrypoint
ENTRYPOINT ["/usr/local/bin/ddb"]

# Default command shows help
CMD ["--help"]

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/usr/local/bin/ddb", "version"]

# Labels for metadata
LABEL maintainer="Charles Watkins <charles@watkinslabs.com>"
LABEL org.opencontainers.image.title="DDB: Enterprise SQL Engine for File-Based Analytics"
LABEL org.opencontainers.image.description="High-performance, stateless SQL engine for querying CSV, JSON, JSONL, YAML, and Parquet files"
LABEL org.opencontainers.image.version="${VERSION}"
LABEL org.opencontainers.image.source="https://github.com/watkinslabs/ddb"
LABEL org.opencontainers.image.documentation="https://watkinslabs.github.io/ddb/"
LABEL org.opencontainers.image.licenses="MIT"