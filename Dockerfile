FROM maven:3-jdk-8

COPY . .
RUN mvn package -Dmaven.test.skip=true
CMD ["java","-jar","target/mq-1.0-SNAPSHOT-jar-with-dependencies.jar"]