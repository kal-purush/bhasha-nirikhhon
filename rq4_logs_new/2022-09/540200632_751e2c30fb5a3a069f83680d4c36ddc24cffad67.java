package com.huiyuan2.cloud.mq.producer;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @description:
 * @author： 灰原二
 * @date: 2022/9/24 9:10
 */
@Slf4j
@AllArgsConstructor
public class KafkaMessageQueue implements MessageQueue{
    // 用kafka properties构建出来的
    private KafkaProducer<String, String> kafkaProducer;

    @Override
    public void send(String topic, String message) {
        send(topic, message, null);
    }

    @Override
    public void send(String topic, String message, String tags) {
        send(topic, message, tags, null);
    }

    @Override
    public void send(String topic, String message, String tags, String key) {
        send(topic, message, tags, key, -1);
    }

    @Override
    public void send(String topic, String message, String tags, String key, Integer delayTimeLevel) {
        if (StringUtils.isNotBlank(tags)) {
            log.warn("Kafka not support parameter: tags -> {}", tags);
        }
        if (delayTimeLevel > 0) {
            log.warn("Kafka not support delay feature: delayTimeLevel -> {}", delayTimeLevel);
        }
        ProducerRecord<String, String> record;
        if (StringUtils.isNotBlank(key)) {
            // kafka，默认来说，你的消息会均匀的分散到你的各个partition里去，数据分片存储
            // 同一个topic里的数据，会分散存储到kafka多台机器里去
            // 均匀分散了以后，数据都是乱序的，你可能需要让某一个业务id对应的多条消息必须进入一个partition里去
            // 加入一个key，key是业务id，确保同一个业务id的数据进入到同一个partition里，有序的
            record = new ProducerRecord<>(topic, key, message);
        } else {
            record = new ProducerRecord<>(topic, message);
        }
        // 还需要接收一个exception的回调，发送结果的需要进行回调通知这样子
        kafkaProducer.send(record, (metadata, exception) -> {
            if (exception == null) {
                log.info("发送消息到Kafka成功");
            } else {
                log.error("发送消息到Kafka失败：", exception);
                throw new RuntimeException(exception);
            }
        });
    }

}