package me.dserrano.blockchain.infra.kafka.node;

import me.dserrano.blockchain.infra.kafka.node.mapper.NodeEventMapper;
import me.dserrano.blockchain.infra.kafka.node.producer.NodeEventProducer;
import me.dserrano.blockchain.node.domain.model.command.PublishNodeCommand;
import me.dserrano.blockchain.node.domain.ports.secondary.PublishNodeCommandHandler;
import org.springframework.stereotype.Component;

@Component
public class KafkaPublishNodeCommandHandler implements PublishNodeCommandHandler {
    private final NodeEventMapper nodeEventMapper;
    private final NodeEventProducer nodeEventProducer;

    public KafkaPublishNodeCommandHandler(NodeEventMapper nodeEventMapper, NodeEventProducer nodeEventProducer) {
        this.nodeEventMapper = nodeEventMapper;
        this.nodeEventProducer = nodeEventProducer;
    }

    @Override
    public void process(PublishNodeCommand command) {
        nodeEventProducer.produce(nodeEventMapper.toNodeEvent(command));
    }
}