# Reporting Services

This repository contains microservices for the reporting platform.

## Services

### Wiz GraphQL Proxy

A proxy service that handles OAuth authentication with Wiz and proxies GraphQL requests.

- Automatically refreshes OAuth tokens
- Caches tokens to minimize auth requests
- Provides a stable endpoint for Hasura to connect to

## Building

```bash
docker build -t harbor.k8s.ng20.org/reporting/wiz-graphql-proxy:latest .
docker push harbor.k8s.ng20.org/reporting/wiz-graphql-proxy:latest
```

## Configuration

Set the following environment variables:
- `WIZ_CLIENT_ID`: Your Wiz OAuth client ID
- `WIZ_CLIENT_SECRET`: Your Wiz OAuth client secret
- `WIZ_GRAPHQL_URL`: Wiz GraphQL endpoint (default: https://api.us48.app.wiz.io/graphql)
- `WIZ_AUTH_URL`: Wiz OAuth token endpoint (default: https://auth.app.wiz.io/oauth/token)