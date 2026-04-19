FROM maven:latest
COPY src/ src
COPY pom.xml .
COPY dblpExample.json /data/dblpExample.json
RUN mvn compile
CMD ["mvn", "exec:java"]
