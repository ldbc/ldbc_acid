FROM gradle:jdk11
COPY . /usr/LDBC
WORKDIR  /usr/LDBC
RUN ./gradlew clean build -x test

CMD ["gradle","test", "--tests", "test.Neo4jAcidTest"]