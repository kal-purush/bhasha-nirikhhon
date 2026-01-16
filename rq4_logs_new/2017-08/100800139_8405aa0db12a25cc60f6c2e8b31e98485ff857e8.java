package stormkafkahdfs.digitalfileanalytics;
//Passes video into a topic using a ByteArray Serializer
//import util.properties packages
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

//import simple producer packages
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;

//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class ByteSerializerAsProducer {
    public static void main(String[] args) throws IOException {
        
        //Assign topicName to string variable
        String topicName = "mukeshtopic";
        // create instance for properties to access producer configs   
        Properties props = new Properties();
        //Assign bootstrap.servers and metadata.broker.list to the primary node in the cluster 
	//(6667)
        props.put("bootstrap.servers", "victoria.com:6667");
        //(6667)
	props.put("metadata.broker.list", "victoria.com:6667");
        //Assign zookeeper.connect to all the nodes in the cluster
	//(2181)
        props.put("zookeeper.connect", "victoria.com:2181");
        //Set acknowledgements for producer requests.      
        props.put("acks", "all");
        //If the request fails, the producer can automatically retry,
        props.put("retries", 0);
        //Specify buffer size in config
        props.put("batch.size", 16384);
        //Reduce the no of requests less than 0   
        props.put("linger.ms", 1);
        //The buffer.memory controls the total amount of memory available to the producer for buffering.   
        props.put("buffer.memory", 33554432);
        //Using a ByteArraySerializer for the value as the video is converted to a ByteArray
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,ByteArraySerializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //Initialize a Kafka Producer
        Producer<String, byte[]> producer = new KafkaProducer<String,byte[]>(props);
        //Using the StringBuilder Class to find the video passed as a command line argument
        //The video is expected to be in the folder /home/ubuntu/interns_files
        StringBuilder sb = new StringBuilder();
        //sb.append("/home/victoria/");
        //sb.append(args[0]);
        sb.append("/home/victoria/Desktop/TEST/uni1.png");
        String output = sb.toString();
        File file = new File(output);
        //Created a ByteArray of the length of size of the Video File
        byte[] b = new byte[(int) file.length()];
        try {
            //Created a FileinputStream pointing to the video file on the local machine
            FileInputStream fileInputStream = new FileInputStream(file);
            //the fileInputStream read the video file as bytes into ByteArray b
            fileInputStream.read(b);
            for(int i = 0; i < b.length; i++) {
        	    System.out.println(b[i]);
     	    }
     	    //Sending the ByteArray to Kafka along to the topic held in the varibale name topicName
       	    producer.send(new ProducerRecord<String,byte[]>(topicName, b));
            fileInputStream.close();
        }
        catch (FileNotFoundException e) {
       	    System.out.println("File Not Found.");
       	    e.printStackTrace();
       	}
        System.out.println("Message sent successfully");
        producer.close();
    }
}