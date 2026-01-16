package org.example.inmemorybroker;

import org.springframework.stereotype.Component;

import java.util.Objects;


@Component
public class ConsumerKafkaCustom {
    private String topicName;
    private int offset;

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConsumerKafkaCustom that = (ConsumerKafkaCustom) o;
        return offset == that.offset && Objects.equals(topicName, that.topicName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicName, offset);
    }

    @Override
    public String toString() {
        return "ConsumerKafkaCustom{" +
                "topicName='" + topicName + '\'' +
                ", offset=" + offset +
                '}';
    }
}