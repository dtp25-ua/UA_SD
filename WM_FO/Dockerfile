FROM eclipse-temurin:21-jdk

WORKDIR /app

COPY lib/ lib/
COPY WM_FO/*.java ./

RUN javac -cp "lib/*" *.java

ENTRYPOINT ["java", "-cp", ".:lib/*"]
