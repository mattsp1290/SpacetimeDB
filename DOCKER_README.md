# SpacetimeDB Docker Support

## Current Docker Support Status

Yes, the repository includes Docker support:

1. **Dockerfile.prod**: A comprehensive multi-stage production Dockerfile that:
   - Builds the SpacetimeDB server (`spacetimedb-standalone`) and CLI (`spacetime`)
   - Creates an optimized production image with all necessary runtime dependencies
   - Includes support for module compilation with Rust and .NET runtimes
   - Provides proper signal handling, health checks, and security configurations

2. **Docker Compose Files**:
   - `docker-compose.yml`: Basic development setup (original)
   - `docker-compose.production.yml`: Production-ready configuration (created)
   - `docker-compose-release.yml`: Production-ready configuration (original)
   - `docker-compose-live.yml`: Live/staging environment setup (original)

3. **Environment Configuration**:
   - `.env.sample`: Sample environment variables for Docker deployment
   - `.dockerignore`: Properly configured to exclude unnecessary files

## Running SpacetimeDB in Docker

### Building the Docker Image

```bash
# Build the production image
docker build -t spacetimedb:latest -f Dockerfile.prod .

# Or use docker-compose
docker-compose -f docker-compose.production.yml build
```

### Running the Server

```bash
# Using docker run
docker run -d \
  -p 3000:3000 \
  -v spacetimedb-data:/var/lib/spacetimedb \
  -v spacetimedb-logs:/var/log/spacetimedb \
  --name spacetimedb \
  spacetimedb:latest

# Using docker-compose
docker-compose -f docker-compose.production.yml up -d
```

### Using the CLI in Docker

The Docker image includes the `spacetime` CLI tool. You can use it in several ways:

```bash
# Execute CLI commands directly
docker exec spacetimedb spacetime --help

# Publish a module to the running SpacetimeDB instance
docker exec spacetimedb spacetime publish my-module --host http://localhost:3000

# Or copy your module into the container first
docker cp ./my-module spacetimedb:/tmp/my-module
docker exec spacetimedb spacetime publish /tmp/my-module --host http://localhost:3000

# Interactive shell access
docker exec -it spacetimedb bash
```

## License Considerations

### Publishing Docker Images to Private Repository

Based on the Business Source License 1.1 (BSL-1.1) terms:

**✅ You CAN publish Docker images to your private repository** because:

1. The license explicitly allows "redistribution" of the Licensed Work
2. Private repository usage is not considered a "Database Service" as defined in the license
3. The Additional Use Grant permits using SpacetimeDB with one instance in production

### Key License Restrictions

You must comply with these conditions:

1. **Single Instance Limit**: Only one SpacetimeDB instance in production
2. **No Database Service**: Cannot offer SpacetimeDB as a commercial service where third parties control table schemas
3. **License Notice**: Must include the BSL-1.1 license notice in your Docker image/repository

### Recommended Docker Image Labels

When publishing to your private repository, include these labels in your Dockerfile:

```dockerfile
LABEL org.opencontainers.image.licenses="BSL-1.1" \
      org.opencontainers.image.source="https://github.com/clockworklabs/SpacetimeDB" \
      org.opencontainers.image.vendor="Clockwork Laboratories, Inc."
```

## Best Practices for Private Repository

1. **Include License**: Always include the `LICENSE.txt` file in your Docker image
2. **Document Changes**: If you modify the Dockerfile, document your changes
3. **Version Tagging**: Use semantic versioning for your Docker images
4. **Security Scanning**: Regularly scan your images for vulnerabilities
5. **Access Control**: Ensure your private repository has appropriate access controls

## Example Production Deployment

```yaml
# docker-compose.production.yml
version: '3.8'

services:
  spacetimedb:
    image: your-registry.com/spacetimedb:1.1.2
    container_name: spacetimedb-prod
    restart: unless-stopped
    ports:
      - "3000:3000"
    volumes:
      - spacetimedb-data:/var/lib/spacetimedb
      - spacetimedb-logs:/var/log/spacetimedb
      - ./config:/etc/spacetimedb
    environment:
      - SPACETIMEDB_LOG_LEVEL=info
      - RUST_LOG=info
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:3000/ping"]
      interval: 30s
      timeout: 3s
      retries: 3
      start_period: 40s
    networks:
      - spacetime-network

volumes:
  spacetimedb-data:
  spacetimedb-logs:

networks:
  spacetime-network:
    driver: bridge
```

## Summary

- ✅ The repository has comprehensive Docker support
- ✅ You can run both the server and CLI in Docker
- ✅ You can publish Docker images to your private repository
- ✅ You can use `spacetime publish` within the Docker container
- ⚠️ Remember to comply with the single production instance limitation
- ⚠️ Cannot use it as a commercial Database Service
