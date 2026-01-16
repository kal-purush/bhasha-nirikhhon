package net.sf.jabb.dstream.kafka;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

import net.sf.jabb.dstream.ReceiveStatus;
import net.sf.jabb.dstream.ex.DataStreamInfrastructureException;

public class KafkaStreamDataSupplierTest {
	static private final MockConsumer<Void, String> mockConsumer;
	static private final List<TopicPartition> topicPartitions = new ArrayList<TopicPartition>();
	static private final String testTopicName = "testTopic";
	static private final int testPartition = 0;
	static private final int firstOffset = 1;
	static private final String firstMsg = "string1";
	static private final String firstOffsetStr = "1";
	static private final String lastOffsetStr = "9"; // 1 + "string1".length()

	static {
		mockConsumer = new MockConsumer<Void, String>(OffsetResetStrategy.LATEST);
		topicPartitions.add(new TopicPartition(testTopicName, testPartition));
		mockConsumer.assign(topicPartitions);
		ConsumerRecord<Void, String> record = new ConsumerRecord<Void, String>(testTopicName, testPartition,
				firstOffset, null, firstMsg);
		mockConsumer.addRecord(record);
		HashMap<TopicPartition, Long> endOffset = new HashMap<TopicPartition, Long>();
		endOffset.put(topicPartitions.get(0), (long) 9);
		mockConsumer.updateEndOffsets(endOffset);
		HashMap<TopicPartition, Long> startOffset = new HashMap<TopicPartition, Long>();
		startOffset.put(topicPartitions.get(0), (long) 1);
		mockConsumer.updateBeginningOffsets(startOffset);
	}

	private KafkaStreamDataSupplier<String> kafkaStream;

	@Before
	public void setUp() {
		kafkaStream = new KafkaStreamDataSupplier<String>(mockConsumer, topicPartitions);
	}

	@Test
	public void testFirstPosition() {
		if (!kafkaStream.firstPosition().equals("1")) {
			fail("kafkaStream.firstPosition()=" + kafkaStream.firstPosition() + ", expect " + firstOffsetStr);
		}
	}

	@Test(expected = DataStreamInfrastructureException.class)
	public void testFirstPositionInstantDuration() throws DataStreamInfrastructureException, InterruptedException {
		kafkaStream.firstPosition(Instant.now());
	}

	@Test
	public void testLastPosition() throws DataStreamInfrastructureException {
		String lastPos = kafkaStream.lastPosition();
		if (!lastPos.equals(lastOffsetStr)) {
			fail("kafkaStream.LastPosition()=" + lastPos + ", expect " + lastOffsetStr);
		}
	}

	@Test(expected = DataStreamInfrastructureException.class)
	public void testEnqueuedTime() throws DataStreamInfrastructureException {
		kafkaStream.enqueuedTime(firstOffsetStr);
	}

	@Test
	public void testNextStartPosition() {
		long next = Long.parseLong(lastOffsetStr) + 1;
		if (!kafkaStream.nextStartPosition(lastOffsetStr).equals(String.valueOf(next))) {
			fail("kafkaStream.nextStartPosition()=" + kafkaStream.nextStartPosition(lastOffsetStr) + ", expected "
					+ String.valueOf(next));
		}
	}

	@Test
	public void testIsInRangeStringString() {
		assertTrue(kafkaStream.isInRange("1", "2"));
		assertTrue(kafkaStream.isInRange("2", null));
		assertTrue(!kafkaStream.isInRange("2", "1"));
	}

	@Test
	public void testIsInRangeInstantInstant() {
		// IsInRange(Instant, Instant) always return false.
		assertTrue(!kafkaStream.isInRange(Instant.MIN, Instant.MAX));
		assertTrue(!kafkaStream.isInRange(Instant.MAX, Instant.MIN));
	}

	@Test
	public void testFetchListOfQsuperMStringStringIntDuration()
			throws DataStreamInfrastructureException, InterruptedException {
		List<String> out = new ArrayList<String>();
		ReceiveStatus ret = kafkaStream.fetch(out, firstOffsetStr, lastOffsetStr, 1, Duration.ofMillis(100));
		if (ret.isOutOfRangeReached()) {
			fail("fetch return out of range error");
		}
		if (!ret.getLastPosition().equals(firstOffsetStr)) {
			fail("fetch last position got " + ret.getLastPosition() + ", expect " + lastOffsetStr);
		}
		if (out.size() != 1) {
			fail("fetch message got " + out.size() + ", expect 1");
		}
		if (!out.get(0).equals(firstMsg)) {
			fail("fetch message got " + out.get(0) + ", expect " + firstMsg);
		}
	}

	@Test(expected = DataStreamInfrastructureException.class)
	public void testFetchListOfQsuperMInstantInstantIntDuration()
			throws DataStreamInfrastructureException, InterruptedException {
		kafkaStream.fetch(null, Instant.MIN, Instant.MAX, Duration.ofMillis(10));
	}

	@Test(expected = DataStreamInfrastructureException.class)
	public void testFetchListOfQsuperMStringInstantIntDuration()
			throws DataStreamInfrastructureException, InterruptedException {
		kafkaStream.fetch(null, Instant.MIN, 1, null);
	}

	@Test(expected = DataStreamInfrastructureException.class)
	public void testStartAsyncReceivingConsumerOfMString() throws DataStreamInfrastructureException {
		kafkaStream.startAsyncReceiving(null, "not implemented");
	}

	@Test(expected = DataStreamInfrastructureException.class)
	public void testStartAsyncReceivingConsumerOfMInstant() throws DataStreamInfrastructureException {
		kafkaStream.startAsyncReceiving(null, Instant.now());
	}

	@Test
	public void testStopAsyncReceiving() {
		kafkaStream.stopAsyncReceiving(null);
	}

	@Test
	public void testReceiveFunctionOfMLongStringString() {
		// TODO, add two or more messages.
	}

	@Test(expected = DataStreamInfrastructureException.class)
	public void testReceiveFunctionOfMLongInstantInstant() throws DataStreamInfrastructureException {
		kafkaStream.receive(null, Instant.MAX, Instant.MAX);
	}

	@Test(expected = DataStreamInfrastructureException.class)
	public void testReceiveFunctionOfMLongStringInstant() throws DataStreamInfrastructureException {
		kafkaStream.receive(null, "1", Instant.MAX);
	}
}