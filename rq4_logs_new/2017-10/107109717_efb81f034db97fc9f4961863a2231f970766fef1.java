package org.wso2;

import java.util.List;

public class StrStrategy {
    private String ConsumerGroup;
    private String strategy;
    private List<String> groupingField;
    private int noOfGroups;

    public String getConsumerGroup() {
        return ConsumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        ConsumerGroup = consumerGroup;
    }

    public String getStrategy() {
        return strategy;
    }

    public void setStrategy(String strategy) {
        this.strategy = strategy;
    }

    public List<String> getGroupingField() {
        return groupingField;
    }

    public void setGroupingField(List<String> groupingField) {
        this.groupingField = groupingField;
    }

    public int getNoOfGroups() {
        return noOfGroups;
    }

    public void setNoOfGroups(int noOfGroups) {
        this.noOfGroups = noOfGroups;
    }
}