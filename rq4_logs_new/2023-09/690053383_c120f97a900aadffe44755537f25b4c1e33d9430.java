package day_4_10;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.velocity.runtime.directive.Foreach;

import java.util.ArrayList;
import java.util.HashMap;

public class MyConsumer {
    public static void main(String[] args) {
        HashMap<String, Object> map = new HashMap<>();
        map.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop11:9092,hadoop12:9092,hadoop13:9092");
        map.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);

        //设置组
        map.put(ConsumerConfig.GROUP_ID_CONFIG,"g2");
        //轮询循环
        map.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,"org.apache.kafka.clients.consumer.RoundRobinAssignor");




        //创建Consumer
        KafkaConsumer<String, String> consumerOne = new KafkaConsumer<>(map);
        //订阅topic数据
        ArrayList<String> list = new ArrayList<>();
        list.add("topicE");
        consumerOne.subscribe(list);

        while (true){
            ConsumerRecords<String, String> poll = consumerOne.poll(2000);
            for (ConsumerRecord p:poll){
                System.out.println(p.value());
            }
        }




    }
}