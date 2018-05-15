FROM openjdk:8-jre
COPY target/metrics-router-0.0.1-SNAPSHOT.jar /opt/metrics-router-0.0.1-SNAPSHOT.jar
CMD ["java","-Djdk.xml.entityExpansionLimit=0", "-jar","/opt/metrics-router-0.0.1-SNAPSHOT.jar", "--spring.config.location=/opt/metrics-router/conf/metrics-router.properties"]
