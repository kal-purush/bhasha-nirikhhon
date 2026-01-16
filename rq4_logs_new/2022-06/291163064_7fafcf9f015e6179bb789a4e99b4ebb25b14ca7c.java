package org.piangles.core.resources;

import java.util.Collection;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.piangles.core.util.Logger;

public class KafkaConsumerRebalanceListener implements ConsumerRebalanceListener
{
	@Override
	public void onPartitionsRevoked(Collection<TopicPartition> partitions)
	{
		for (TopicPartition partition : partitions)
		{
			Logger.getInstance().info("onPartitionsRevoked: " + partition);
		}
	}

	@Override
	public void onPartitionsAssigned(Collection<TopicPartition> partitions)
	{
		for (TopicPartition partition : partitions)
		{
			Logger.getInstance().info("onPartitionsAssigned: " + partition);
		}
	}
}