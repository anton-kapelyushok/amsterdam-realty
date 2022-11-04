FROM arm64v8/openjdk:latest

COPY "target/amsterdam-realty-0.0.1-SNAPSHOT.jar" /app.jar
COPY ".token" "/.token"

ENV DB_PATH=jdbc:h2:file:/db/data

ENTRYPOINT ["java", "-jar", "/app.jar"]