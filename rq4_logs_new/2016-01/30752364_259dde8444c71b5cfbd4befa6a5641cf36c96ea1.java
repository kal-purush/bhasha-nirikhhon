package net.sf.jabb.dstream.kafka;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import net.sf.jabb.dstream.ReceiveStatus;
import net.sf.jabb.dstream.SimpleReceiveStatus;
import net.sf.jabb.dstream.StreamDataSupplier;
import net.sf.jabb.dstream.ex.DataStreamInfrastructureException;

public class KafkaStreamDataSupplier<M> implements StreamDataSupplier<M> {
	private KafkaConsumer<Void, M> consumer;
	private TopicPartition subscribedPartition;

	KafkaStreamDataSupplier(Properties properties, List<TopicPartition> partitions) {
		Validate.isTrue(partitions.size() == 1);
		subscribedPartition = partitions.get(0);
		consumer = new KafkaConsumer<Void, M>(properties);
		consumer.assign(partitions);
	}
	@Override
	public String firstPosition() {
		long curPos = consumer.position(subscribedPartition);
		// move to beginning of the topic
		consumer.seekToBeginning(subscribedPartition);
		Long firstPos = consumer.position(subscribedPartition);
		consumer.seek(subscribedPartition, curPos);
		return firstPos.toString();
	}

	@Override
	public String firstPosition(Instant enqueuedAfter, Duration waitForArrival)
			throws InterruptedException, DataStreamInfrastructureException {
		throw new DataStreamInfrastructureException("Kafka do not support enqueue timestamp");
	}

	@Override
	public String lastPosition() throws DataStreamInfrastructureException {
		long curPos = consumer.position(subscribedPartition);
		// move to end of the topic
		consumer.seekToEnd(subscribedPartition);
		long lastPos = consumer.position(subscribedPartition);
		consumer.seek(subscribedPartition, curPos);
		return String.valueOf(lastPos);
	}

	@Override
	public Instant enqueuedTime(String position) throws DataStreamInfrastructureException {
		throw new DataStreamInfrastructureException("Kafka do not support enqueue timestamp");
	}

	@Override
	public String nextStartPosition(String previousEndPosition) {
		Long nextPos = Long.parseLong(previousEndPosition) + 1;
		return nextPos.toString();
	}

	@Override
	public boolean isInRange(String position, String endPosition) {
		Validate.isTrue(position != null, "position cannot be null");
		if (endPosition == null) {
			return true;
		}
		return Long.parseLong(position) <= Long.parseLong(endPosition);
	}

	@Override
	public boolean isInRange(Instant enqueuedTime, Instant endEnqueuedTime) {
		// TODO throw exception.
		return false;
	}

	@Override
	public ReceiveStatus fetch(List<? super M> list, String startPosition, String endPosition, int maxItems,
			Duration timeoutDuration) throws InterruptedException, DataStreamInfrastructureException {
		Long startPos = Long.parseLong(startPosition);
		Long endPos = Long.parseLong(endPosition);
		consumer.seek(subscribedPartition, startPos);
		long opMaxTime = System.currentTimeMillis() + timeoutDuration.toMillis();
		while (true) {
			// Since poll can't specify maxItems, we just wait for timeout. We
			// can also do busy poll and check each return until maxItem reached
			// or timeout.
			int count = 0;
			boolean outOfRange = false;
			Long lastPos = (long) 0;
			long opNow = System.currentTimeMillis();
			if (opNow >= opMaxTime) {
				return new SimpleReceiveStatus(lastPos.toString(), null, outOfRange);
			}
			ConsumerRecords<Void, M> records = consumer.poll(opMaxTime - opNow);
			Iterator<ConsumerRecord<Void, M>> it = records.iterator();
			while (it.hasNext() && count < maxItems) {
				ConsumerRecord<Void, M> record = it.next();
				if (record.offset() > endPos) {
					outOfRange = true;
					return new SimpleReceiveStatus(lastPos.toString(), null, outOfRange);
				}
				list.add(record.value());
				lastPos = record.offset();
			}

		}
	}

	@Override
	public ReceiveStatus fetch(List<? super M> list, Instant startEnqueuedTime, Instant endEnqueuedTime, int maxItems,
			Duration timeoutDuration) throws InterruptedException, DataStreamInfrastructureException {
		throw new DataStreamInfrastructureException("Kafka do not support enqueue timestamp");
	}

	@Override
	public ReceiveStatus fetch(List<? super M> list, String startPosition, Instant endEnqueuedTime, int maxItems,
			Duration timeoutDuration) throws InterruptedException, DataStreamInfrastructureException {
		throw new DataStreamInfrastructureException("Kafka do not support enqueue timestamp");
	}

	@Override
	public String startAsyncReceiving(Consumer<M> receiver, String startPosition)
			throws DataStreamInfrastructureException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String startAsyncReceiving(Consumer<M> receiver, Instant startEnqueuedTime)
			throws DataStreamInfrastructureException {
		throw new DataStreamInfrastructureException("Kafka do not support enqueue timestamp");
	}

	@Override
	public void stopAsyncReceiving(String id) {
		// TODO Auto-generated method stub

	}

	@Override
	public ReceiveStatus receive(Function<M, Long> receiver, String startPosition, String endPosition)
			throws DataStreamInfrastructureException {
		long startPos = Long.parseLong(startPosition);
		long endPos = Long.parseLong(endPosition);
		consumer.seek(subscribedPartition, startPos);
		boolean outOfRange = false;
		long millisecondLeft = receiver.apply(null);
		Long lastPos = (long) -1;
		while (true) {
			// poll with 1 millisecond timeout.
			ConsumerRecords<Void, M> records = consumer.poll(1);
			Iterator<ConsumerRecord<Void, M>> it = records.iterator();
			while (it.hasNext()) {
				ConsumerRecord<Void, M> record = it.next();
				if (record.offset() <= endPos) {
					millisecondLeft = receiver.apply(record.value());
					lastPos = record.offset();
					if (millisecondLeft < 0) {
						return new SimpleReceiveStatus(lastPos.toString(), null, outOfRange);
					}
				} else {
					outOfRange = true;
					return new SimpleReceiveStatus(lastPos.toString(), null, outOfRange);
				}
			}
		}
	}

	@Override
	public ReceiveStatus receive(Function<M, Long> receiver, Instant startEnqueuedTime, Instant endEnqueuedTime)
			throws DataStreamInfrastructureException {
		throw new DataStreamInfrastructureException("Kafka do not support enqueue timestamp");
	}

	@Override
	public ReceiveStatus receive(Function<M, Long> receiver, String startPosition, Instant endEnqueuedTime)
			throws DataStreamInfrastructureException {
		throw new DataStreamInfrastructureException("Kafka do not support enqueue timestamp");
	}

	@Override
	public void start() throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void stop() throws Exception {
		// TODO Auto-generated method stub

	}

}