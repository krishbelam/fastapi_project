# Future Release Items

1. Persistence

* Swap fake_users_db with SQLAlchemy async + Alembic migrations
* Swap job_status_store with Redis so job state survives restarts and works across multiple workers

2. Auth

* Replace fake get_current_user with JWT decode (python-jose) + OAuth2PasswordBearer
* Add refresh token rotation and role-based access control

3. Observability

* Structured JSON logging (structlog) instead of plain text
* Prometheus metrics (message rates, job durations, semaphore saturation)
* OpenTelemetry traces across HTTP → RabbitMQ → handler

4. RabbitMQ

* Dead-letter queue consumer to handle and alert on poisoned messages
* Publisher confirms (async acks from broker) to guarantee delivery
* Multiple queue bindings for different event types routed to different handlers

5. Resilience

* Retry with exponential backoff in handlers (tenacity)
* Circuit breaker on external HTTP calls (httpx + stamina)

6. Testing

* Contract tests for RabbitMQ message schemas (pact)
* Load tests with Locust to validate semaphore tuning

7. Deployment

* Dockerfile + Kubernetes Deployment with liveness probe hitting /health/rabbitmq
* Horizontal scaling — multiple pods, one RabbitMQ connection each, shared Redis for job state

