# imagen base
FROM eclipse-temurin:21-jdk

# directorio en el contenedor
WORKDIR /app

# copiamos las bibliotecas y el codigo fuente de central
COPY lib/ lib/
COPY WM_Central/*.java ./

# compilamos el codigo
RUN javac -cp "lib/*" *.java

# Arrancamos Central
CMD ["sh", "-c", "exec java -cp '.:lib/*' WM_Central \"$CENTRAL_PORT\" \"$KAFKA_HOST\" \"$KAFKA_PORT\" \"$DB_PATH\" \"$TIMEOUT_MS\""]