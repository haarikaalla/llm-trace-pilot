# syntax=docker/dockerfile:1.6
# Single image, multiple entrypoints (selected via SERVICE_ROLE) — keeps the
# microservices in lock-step on one tested artifact.
FROM python:3.12-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
RUN groupadd -r app && useradd -r -g app -d /app app

FROM base AS deps
COPY requirements.txt requirements-platform.txt ./
RUN pip install --upgrade pip && pip install -r requirements.txt -r requirements-platform.txt

FROM deps AS runtime
COPY --chown=app:app tracer ./tracer
COPY --chown=app:app evaluator ./evaluator
COPY --chown=app:app prompts ./prompts
COPY --chown=app:app dashboard ./dashboard
COPY --chown=app:app observability ./observability
COPY --chown=app:app examples ./examples
COPY --chown=app:app deploy/docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh && mkdir -p /data && chown app:app /data
USER app
ENV OBSERVABILITY_DB=/data/agentscope.db AGENTSCOPE_DB=/data/agentscope.db \
    SERVICE_ROLE=dashboard PORT=8080
EXPOSE 8080 8081 8082
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s CMD /entrypoint.sh healthcheck
ENTRYPOINT ["/entrypoint.sh"]
