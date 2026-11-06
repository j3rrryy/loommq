FROM eclipse-temurin:25-jdk-alpine AS builder

WORKDIR /app
COPY gradle gradle
COPY gradlew .
COPY build.gradle.kts .
COPY settings.gradle.kts .
RUN ./gradlew --no-daemon dependencies

COPY src src
RUN ./gradlew --no-daemon installDist

FROM eclipse-temurin:25-jre-alpine

RUN addgroup -S loommq && adduser -S loommq -G loommq

WORKDIR /app
COPY --from=builder /app/build/install/loommq /app
RUN mkdir -p /app/data && chown -R loommq:loommq /app

USER loommq

EXPOSE 1883

ENV JAVA_OPTS="-XX:+UseZGC -XX:MaxRAMPercentage=75.0 -XX:+ExitOnOutOfMemoryError"

ENTRYPOINT ["/app/bin/loommq", "--config", "/app/config.yaml"]
