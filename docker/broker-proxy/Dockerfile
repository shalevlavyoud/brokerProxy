# =============================================================================
# Broker Proxy — Multi-stage Dockerfile
# Java 21 + Vert.x  |  Maven multi-module build (maven-shade-plugin fat jar)
# =============================================================================

# -----------------------------------------------------------------------------
# Stage 1: Build
# -----------------------------------------------------------------------------
FROM eclipse-temurin:25-jdk-alpine AS builder

WORKDIR /workspace

# ── Copy Maven wrapper + root POM first (layer-cache resolution step) ────────
COPY .mvn .mvn/
COPY mvnw ./
COPY pom.xml ./
COPY broker-proxy/pom.xml ./broker-proxy/

# Make wrapper executable and resolve dependencies for this module only
RUN chmod +x mvnw \
 && ./mvnw dependency:go-offline -pl broker-proxy -am --no-transfer-progress -q

# ── Copy source and build ─────────────────────────────────────────────────────
COPY broker-proxy/src ./broker-proxy/src/

RUN ./mvnw package -pl broker-proxy -am -DskipTests --no-transfer-progress -q \
 && ls -lh broker-proxy/target/

# -----------------------------------------------------------------------------
# Stage 2: Runtime
# -----------------------------------------------------------------------------
FROM eclipse-temurin:25-jre-alpine AS runtime

LABEL maintainer="platform-team" \
      service="broker-proxy" \
      version="1.0"

# Create unprivileged user
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /app

# Copy fat jar from builder
# maven-shade-plugin default: broker-proxy-{version}.jar  (no classifier)
# Adjust glob if your pom.xml uses a different finalName or classifier
COPY --from=builder /workspace/broker-proxy/target/broker-proxy-*.jar app.jar

# Create runtime directories
RUN mkdir -p /app/config /app/logs \
 && chown -R appuser:appgroup /app

USER appuser

EXPOSE 8080

# JVM tuning for containerised environments (Java 21)
ENV JAVA_OPTS="\
  -XX:+UseContainerSupport \
  -XX:MaxRAMPercentage=75.0 \
  -XX:+UseG1GC \
  -XX:G1HeapRegionSize=4m \
  -XX:+ExitOnOutOfMemoryError \
  -Djava.net.preferIPv4Stack=true \
  -Dvertx.logger-delegate-factory-class-name=io.vertx.core.logging.SLF4JLogDelegateFactory"

HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=5 \
  CMD wget -qO- http://localhost:8080/health || exit 1

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
