mvn clean package -DskipTests
java -jar target/akka-java.jar "data_100000.txt" "result1_1000.txt" "result2_1000.txt" "result3_1000.txt"
