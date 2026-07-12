# 15. Design an API Gateway

## Requirements

### Functional
- Route incoming requests to the correct backend service
- Authenticate and authorize every request before forwarding
- Enforce rate limiting per client/user/API key
- Transform requests and responses (headers, payloads, protocol)
- Aggregate multiple backend calls into a single client response
- Support WebSocket and streaming in addition to HTTP

### Non-Functional
- **Low latency**: gateway overhead must be < 5ms p99 (not a bottleneck)
- **High availability**: single point of entry — must never go down
- **Scalability**: handle millions of requests per second
- **Observability**: every request logged, traced, and metered
- Scale: large platform — 1M+ requests/second at peak

---

## Scale Estimation

```
Requests:       1,000,000 requests/second
Avg payload:    2 KB
Inbound bandwidth: 1M × 2 KB = 2 GB/second

Gateway instances needed:
  One gateway instance (4-core, 8 GB): ~50,000 req/s (mostly network I/O)
  1M req/s / 50K per instance = 20 instances minimum
  Deploy 40 for 2× headroom and rolling deploys

Latency budget:
  Target: < 5ms added by gateway
  Auth token validation: ~0.5ms (local JWT verify or Redis lookup)
  Rate limit check:      ~0.5ms (Redis INCR)
  Routing + proxy:       ~1–2ms (network hop to backend)
  Logging (async):       0ms (non-blocking)
```

---

## High-Level Architecture

```mermaid
flowchart LR
    Client["Client\n(Mobile / Browser / 3rd Party)"]
    LB["Load Balancer"]

    subgraph GW["API Gateway Cluster"]
        G1["Gateway Instance 1"]
        G2["Gateway Instance 2"]
        G3["Gateway Instance N"]
    end

    Auth["Auth Service"]
    RL["Rate Limit Store\n(Redis)"]
    SD["Service Registry\n(Consul)"]

    SvcA["User Service"]
    SvcB["Order Service"]
    SvcC["Notification Service"]

    Client --> LB
    LB --> G1 & G2 & G3
    G1 & G2 & G3 -->|"token validation"| Auth
    G1 & G2 & G3 -->|"rate limit check"| RL
    G1 & G2 & G3 -->|"service lookup"| SD
    G1 --> SvcA
    G2 --> SvcB
    G3 --> SvcC
```

---

## Core Components

### 1. Request Pipeline — How Every Request Flows Through the Gateway

Every request passes through a sequential **middleware pipeline**. Each step can short-circuit (return early) or pass the request to the next step:

```
Incoming request
  │
  ▼
1. TLS Termination     — decrypt HTTPS; backend services use plain HTTP internally
  │
  ▼
2. Request Parsing     — extract method, path, headers, body
  │
  ▼
3. Authentication      — validate JWT or API key; reject with 401 if invalid
  │
  ▼
4. Authorization       — check if this token has permission for this route; 403 if not
  │
  ▼
5. Rate Limiting       — check quota; reject with 429 if exceeded
  │
  ▼
6. Request Transform   — rewrite headers, inject user context, normalise payload
  │
  ▼
7. Routing             — determine which backend service handles this request
  │
  ▼
8. Backend Call        — forward request; handle timeout, retry, circuit break
  │
  ▼
9. Response Transform  — filter sensitive fields, normalise response format
  │
  ▼
10. Logging & Tracing  — emit access log, metrics, distributed trace span (async)
  │
  ▼
Client response
```

The pipeline is configurable per route — a public health-check endpoint skips auth; an admin route adds extra authorization checks.

```csharp
// ASP.NET Core middleware pipeline (order matters):
app.UseMiddleware<TlsTerminationMiddleware>();
app.UseMiddleware<AuthenticationMiddleware>();
app.UseMiddleware<AuthorizationMiddleware>();
app.UseMiddleware<RateLimitingMiddleware>();
app.UseMiddleware<RequestTransformMiddleware>();
app.UseMiddleware<RoutingMiddleware>();
app.UseMiddleware<ResponseTransformMiddleware>();
app.UseMiddleware<ObservabilityMiddleware>();
```

---

### 2. Authentication — JWT and API Keys

**JWT (JSON Web Token)** for user-facing clients:

```
Client sends:  Authorization: Bearer eyJhbGciOiJSUzI1NiJ9...

Gateway:
  1. Split token into header.payload.signature
  2. Verify signature using the Auth Service's public key (RSA or ECDSA)
  3. Check expiry (exp claim) — reject if expired
  4. Extract user_id, roles, scopes from payload
  5. Inject as headers for downstream services: X-User-Id, X-User-Roles
```

JWT verification is done **locally** on the gateway using the cached public key — no network call to the Auth Service per request. The public key is fetched once at startup and refreshed periodically (key rotation).

```csharp
public class AuthenticationMiddleware
{
    public async Task InvokeAsync(HttpContext context, RequestDelegate next)
    {
        var token = context.Request.Headers["Authorization"]
            .ToString().Replace("Bearer ", "");

        try
        {
            var principal = _jwtValidator.ValidateToken(token);  // local verification
            context.Items["UserId"] = principal.FindFirst("sub")?.Value;
            context.Items["Roles"]  = principal.FindFirst("roles")?.Value;
            await next(context);
        }
        catch (SecurityTokenException)
        {
            context.Response.StatusCode = 401;
            await context.Response.WriteAsync("Unauthorized");
        }
    }
}
```

**API keys** for third-party developers:
- API key is sent in `X-Api-Key` header
- Gateway looks up the key in Redis (hashed, never stored in plain text): `GET apikey:{hash(key)}` → `{ client_id, rate_limit_tier, allowed_scopes }`
- One Redis lookup per request (~0.5ms); result is cached locally for 60 seconds

---

### 3. Rate Limiting — Protecting Backend Services

Rate limiting prevents any single client from overwhelming the system. Implemented using the **sliding window** algorithm in Redis:

```csharp
public async Task<bool> IsAllowedAsync(string clientId, string route, int limitPerMinute)
{
    var key = $"ratelimit:{clientId}:{route}:{DateTimeOffset.UtcNow.ToUnixTimeSeconds() / 60}";

    var count = await _redis.StringIncrementAsync(key);
    if (count == 1)
        await _redis.KeyExpireAsync(key, TimeSpan.FromMinutes(2)); // expire after the window

    if (count > limitPerMinute)
    {
        // Set Retry-After header so client knows when to retry
        context.Response.Headers["Retry-After"] = "60";
        return false;  // → 429 Too Many Requests
    }
    return true;
}
```

**Rate limit tiers** (configured per API key tier):

| Tier | Limit | Who |
|---|---|---|
| Free | 100 req/min | Public developers |
| Standard | 1,000 req/min | Paying customers |
| Premium | 10,000 req/min | Enterprise |
| Internal | Unlimited | Internal services |

Rate limits can be applied at multiple granularities: per client, per IP, per route, per user. A single request might be checked against all four simultaneously.

---

### 4. Routing — Directing Requests to Backend Services

The gateway maintains a **route table** that maps URL patterns to backend services:

```
Route table:
  GET  /api/v1/users/**         → User Service        (http://user-svc:8080)
  POST /api/v1/orders           → Order Service        (http://order-svc:8081)
  GET  /api/v1/products/**      → Product Service      (http://product-svc:8082)
  WS   /ws/notifications        → Notification Service (http://notif-svc:8083)
  GET  /api/v1/health           → (gateway itself, no backend call)
```

Routes are loaded from a **Service Registry** (Consul or etcd) and refreshed dynamically. When a new service is deployed, it registers itself with Consul — the gateway picks up the new route within seconds without a restart.

**Path rewriting**: the gateway can strip the API version prefix before forwarding:
```
Client:   GET /api/v1/users/42
Gateway:  GET /users/42   (forwards to User Service without the /api/v1 prefix)
```

This decouples the public API versioning from internal service paths.

---

### 5. Backend Resilience — Timeouts, Retries, Circuit Breaker

The gateway protects clients from slow or failing backends:

**Timeout**: if the backend does not respond within 5 seconds, return 504 Gateway Timeout. Never let a slow backend hold a gateway connection open indefinitely — that would exhaust the gateway's connection pool.

**Retry**: on transient failures (502, 503, network timeout), retry once against a different instance. Only safe for idempotent requests (GET, PUT) — never retry POST automatically.

**Circuit breaker**: if a backend is failing consistently (> 50% error rate in 10 seconds), stop sending requests to it and return 503 immediately. Check again after 30 seconds (half-open state):

```
CLOSED (normal) → error rate > 50% → OPEN (fail fast, no backend calls)
                                        ↓ 30 seconds
                                      HALF-OPEN (try one request)
                                        ↓ success → CLOSED
                                        ↓ failure → OPEN again
```

This prevents a failing backend from cascading failures to the gateway and all other clients.

---

### 6. Request and Response Transformation

**Request transformation** — enrich the request before forwarding:
```
Add headers:
  X-User-Id:        42                  (extracted from JWT)
  X-Request-Id:     uuid-abc-123        (generated for distributed tracing)
  X-Forwarded-For:  203.0.113.42        (original client IP, for rate limiting downstream)
  X-Api-Version:    v1                  (so backend knows which API version was called)

Strip headers:
  Authorization     (JWT token — backend does not need it; user context is in X-User-Id)
```

**Response transformation** — clean up the response before returning to client:
```
Remove internal headers:
  X-Internal-Pod-Id, X-Backend-Version  (internal debug info, not for clients)

Add CORS headers (if applicable):
  Access-Control-Allow-Origin: *

Normalise error format:
  Backend returns: { "error": "user not found", "code": 404 }
  Gateway returns: { "status": 404, "message": "User not found", "request_id": "uuid-abc-123" }
```

---

### 7. API Aggregation — Combining Multiple Backend Calls

Some client requests require data from multiple services. Instead of making 3 separate calls from the client, the gateway (or a dedicated BFF — Backend For Frontend) aggregates them:

```csharp
public async Task<DashboardResponse> GetDashboardAsync(long userId)
{
    // Fan out to three services in parallel:
    var userTask    = _userClient.GetUserAsync(userId);
    var ordersTask  = _orderClient.GetRecentOrdersAsync(userId, limit: 5);
    var notifTask   = _notifClient.GetUnreadCountAsync(userId);

    await Task.WhenAll(userTask, ordersTask, notifTask);

    return new DashboardResponse(
        User:          await userTask,
        RecentOrders:  await ordersTask,
        UnreadNotifs:  await notifTask
    );
}
```

Three backend calls fire in parallel; the client receives one response. Without aggregation, the client would need three round trips — 3× the latency and 3× the auth overhead.

---

### 8. Observability — Logs, Metrics, Traces

Every request through the gateway emits three signals (asynchronously — never blocking the response):

**Access log** (structured JSON, shipped to log aggregator):
```json
{
  "timestamp":    "2026-07-11T10:00:00.123Z",
  "request_id":   "uuid-abc-123",
  "method":       "GET",
  "path":         "/api/v1/users/42",
  "status":       200,
  "latency_ms":   12,
  "client_id":    "app-mobile-ios",
  "user_id":      "42",
  "backend":      "user-svc",
  "backend_ms":   10
}
```

**Metrics** (Prometheus):
- `gateway_requests_total{route, status, method}` — request count
- `gateway_latency_ms{route, p50, p95, p99}` — latency histogram
- `gateway_rate_limited_total{client_id}` — rate limit hits
- `gateway_circuit_breaker_state{backend}` — open/closed/half-open

**Distributed trace** (OpenTelemetry → Jaeger):
- Gateway creates a root trace span for each request
- Injects `traceparent` header when forwarding to backend
- Backend services add child spans — the full call tree is visible in Jaeger

---

## Data Model

The gateway is stateless — no persistent database. State lives in:

**Redis** (shared across all gateway instances):
```
apikey:{hash}          → { client_id, tier, scopes }   TTL: 60s (local cache refreshed from here)
ratelimit:{id}:{window} → request count                TTL: 2 minutes
circuit:{backend}      → "open" | "closed"             TTL: 30s (auto-expires to half-open)
```

**Service Registry** (Consul):
```
/services/user-svc/instances  → [{ host, port, health }]
/services/order-svc/instances → [{ host, port, health }]
/routes                       → route table (path patterns → service names)
```

---

## API Design

The gateway does not have its own API — it is transparent to clients. Clients call backend service endpoints directly through it:

```
Client calls:    GET https://api.example.com/api/v1/users/42
Gateway routes:  GET http://user-svc:8080/users/42
Client sees:     200 OK { user object }
```

The gateway's management plane (for ops teams) exposes:
```
GET  /gateway/routes              → list all configured routes
POST /gateway/routes              → add a new route
PUT  /gateway/rate-limits/{id}    → update rate limit for a client
GET  /gateway/metrics             → Prometheus metrics endpoint
POST /gateway/circuit/{svc}/reset → manually reset a circuit breaker
```

---

## Key Challenges & Solutions

### Challenge 1: Gateway becomes a single point of failure

Every request passes through the gateway — if it goes down, the entire platform is inaccessible.

**Solution**: run multiple gateway instances behind a load balancer. The load balancer itself is a managed service (AWS ALB, GCP Load Balancer) with built-in redundancy. Gateway instances are stateless — any instance handles any request. Health checks remove unhealthy instances within seconds.

### Challenge 2: Auth adds latency on every request

Calling the Auth Service for every request adds a network hop.

**Solution**: local JWT verification (no network call). The gateway holds the Auth Service's public key in memory and verifies tokens locally in < 1ms. The public key is rotated on a schedule (fetched from a JWKS endpoint). Only opaque tokens (session tokens) require a network call — and those results are cached in Redis for 60 seconds.

### Challenge 3: Redis becomes a bottleneck for rate limiting

1M req/s × one Redis call each = 1M Redis operations/second — a single Redis node cannot handle this.

**Solution**: two-level rate limiting:
1. **Local counter** (in-process, per gateway instance): allow up to `limit / num_instances` locally without hitting Redis. Resets every second.
2. **Global Redis counter**: checked every 10 requests (not every request) to enforce the global limit precisely.

Most rate limiting is absorbed locally; Redis only arbitrates edge cases near the limit.

### Challenge 4: Route configuration changes require restarts

Hardcoded routes mean adding a new service requires a gateway redeploy.

**Solution**: dynamic routing via Consul. Routes are stored in Consul's key-value store. Gateway instances watch for changes using Consul's long-poll API — a route change propagates to all instances within seconds. New services self-register; removed services deregister automatically (via health check failure).

---

## Trade-offs

| Decision | Choice | Why | Alternative |
|---|---|---|---|
| Auth method | Local JWT verification | No network call per request; < 1ms | Remote token introspection (accurate but adds latency) |
| Rate limit storage | Redis (shared) | Consistent global limits across instances | Local only (fast but per-instance limits allow 2× overage) |
| Rate limit algorithm | Sliding window | Smooth enforcement; no burst at window boundary | Fixed window (simpler but allows 2× burst at boundary) |
| Routing config | Consul (dynamic) | Route changes without restarts | Static config file (simple but requires redeploy) |
| Resilience | Circuit breaker | Fail fast; prevents cascade | Infinite retries (would amplify load on a struggling backend) |
| Aggregation | Gateway-level BFF | Reduces client round trips | Client-side aggregation (more round trips, more latency) |
| CAP position | **AP** | Better to return a slightly stale rate limit count than block all requests | CP (unnecessary — allowing a few extra requests briefly is acceptable) |

---

## Sequence Diagrams

**Authenticated request through the gateway pipeline**

```mermaid
sequenceDiagram
    participant C as Client
    participant GW as API Gateway
    participant Redis as Redis
    participant US as User Service

    C->>GW: GET /api/v1/users/42 + JWT token
    GW->>GW: verify JWT signature locally
    GW->>Redis: INCR ratelimit:client-1:window
    Redis-->>GW: count=5 (within limit)
    GW->>US: GET /users/42 + X-User-Id:42
    US-->>GW: 200 OK user object
    GW->>GW: strip internal headers, add request-id
    GW-->>C: 200 OK user object
    Note over GW: access log + trace emitted async
```

**Circuit breaker — backend service failing**

```mermaid
sequenceDiagram
    participant C as Client
    participant GW as API Gateway
    participant OS as Order Service

    C->>GW: GET /api/v1/orders/99
    GW->>OS: forward request
    OS-->>GW: 503 Service Unavailable
    GW->>GW: record failure (3 of 5 threshold)

    C->>GW: GET /api/v1/orders/100
    GW->>OS: forward request
    OS-->>GW: timeout
    GW->>GW: record failure (5 of 5 — circuit opens)

    C->>GW: GET /api/v1/orders/101
    GW-->>C: 503 immediately (circuit OPEN, no backend call)
    Note over GW: retries blocked for 30 seconds

    Note over GW: 30 seconds later — circuit HALF-OPEN
    C->>GW: GET /api/v1/orders/102
    GW->>OS: one probe request
    OS-->>GW: 200 OK (backend recovered)
    GW->>GW: circuit CLOSED — normal traffic resumes
    GW-->>C: 200 OK order object
```
