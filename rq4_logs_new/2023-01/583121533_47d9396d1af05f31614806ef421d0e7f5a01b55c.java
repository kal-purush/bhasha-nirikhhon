package me.dserrano.blockchain.infra.kafka.node.mapper;

import me.dserrano.blockchain.infra.kafka.node.model.NodeEvent;
import me.dserrano.blockchain.infra.kafka.node.model.NodeEventMother;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(classes = NodeEventMapperImpl.class)
class NodeEventMapperTest {

    @Autowired
    private NodeEventMapper nodeEventMapper;

    @Test
    @DisplayName("Mapping a command to a node")
    void mappingTest() {
        NodeEvent result = nodeEventMapper.toNodeEvent(NodeEventMother.publishNodeCommand);
        Assertions.assertEquals(NodeEventMother.publishNodeCommand.node().id(), result.id());
        Assertions.assertEquals(NodeEventMother.publishNodeCommand.node().host(), result.host());
        Assertions.assertEquals(NodeEventMother.publishNodeCommand.node().port(), result.port());
        Assertions.assertEquals(NodeEventMother.publishNodeCommand.dateTime(), result.dateTime());
    }
}