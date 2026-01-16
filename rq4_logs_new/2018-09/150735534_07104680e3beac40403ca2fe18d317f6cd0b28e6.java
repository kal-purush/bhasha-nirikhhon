package com.upgrad.kafka.consumer;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

//import simple producer packages
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.upgrad.kafka.consumer.LoginDetails;

public class KafkaConsumerDemo {

    public static void main(String[] args) throws Exception {
    	// Input Arguments Validation handling.
        if (args.length < 2) {
            System.out.println("Usage: consumer <topic> <groupname>");
            return;
        }

        //Creating Properties Object and reading from configurations file.
        Properties configProps = new Properties();
        try (InputStream resourceStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("config.properties")) {
            configProps.load(resourceStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        // Setting Database URL and JDBC Driver.
        final String JDBC_DRIVER = configProps.getProperty("com.mysql.jdbc.Driver");
        final String DB_URL = "jdbc:mysql://" + configProps.getProperty("mysql_server") + "/" + configProps.getProperty("mysql_database");
        System.out.println(DB_URL);

        // Setting Database credentials
        final String USER = configProps.getProperty("mysql_user");
        final String PASS = configProps.getProperty("mysql_password");;

        // Storing Input arguments recieved from the Application Interface
        String topic = args[0].toString();
        String group = args[1].toString();
        
        // Create instance for properties to access producer configs
        Properties props = new Properties();
        props.put("bootstrap.servers", configProps.getProperty("kafka_server"));
        props.put("group.id", group);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.connect.json.JsonDeserializer");

        KafkaConsumer<String, JsonNode> consumer = new KafkaConsumer<String, JsonNode>(props);

        consumer.subscribe(Arrays.asList(topic));
        System.out.println("Subscribed to topic " + topic);
        Duration timeout = Duration.ofMillis(2000);

        Class.forName("com.mysql.jdbc.Driver");

        // STEP 3: Open a connection
        System.out.print("\nConnecting to database...");
        Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
        System.out.println(" SUCCESS!\n");
        String query = " insert into login_details (user_name, user_agent, country, os_name, os_version,browser_name,browser_version,device_type,ipaddress,time_zone,product,login_time)" + " values (?, ?, ?, ?, ?,?, ?, ?, ?,?, ?, ?)";
        ObjectMapper objectMapper = new ObjectMapper();
        // create the mysql insert preparedstatement
        PreparedStatement preparedStmt = conn.prepareStatement(query);

        while (true) {
            ConsumerRecords<String, JsonNode> records = consumer.poll(timeout);
            for (ConsumerRecord<String, JsonNode> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
                
                LoginDetails loginPojo = objectMapper.treeToValue(record.value(), LoginDetails.class);

                preparedStmt.setString(1, loginPojo.getUsername());
                preparedStmt.setString(2, loginPojo.getUserAgent());
                preparedStmt.setString(3, loginPojo.getCountry());
                preparedStmt.setString(4, loginPojo.getOs());
                preparedStmt.setString(5, loginPojo.getOsVersion());
                preparedStmt.setString(6, loginPojo.getBrowser());
                preparedStmt.setString(7, loginPojo.getBrowserVersion());
                preparedStmt.setString(8, loginPojo.getDeviceType());
                preparedStmt.setString(9, loginPojo.getIpAddress());
                preparedStmt.setString(10, loginPojo.getTz());
                preparedStmt.setString(11, loginPojo.getProduct());
                preparedStmt.setString(12, loginPojo.getLoginTime());

                // execute the preparedstatement
                preparedStmt.execute();

            }
        }
    }

}