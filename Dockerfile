# To change this license header, choose License Headers in Project Properties.
# To change this template file, choose Tools | Templates
# and open the template in the editor.
# Alpine Linux with OpenJDK JRE
FROM openjdk:8-jre-alpine
# copy jar into image
COPY target/k-octopus-processors-0.7.3-jar-with-dependencies.jar /k-octopus-processors.jar 
# run application with this command line 
CMD ["/usr/bin/java", "-jar", "/k-octopus-processors.jar"]
