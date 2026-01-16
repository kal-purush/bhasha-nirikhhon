/**
 * 
 */
package net.sf.jabb.dstream.kinesis;

import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import net.sf.jabb.dstream.ReceiveStatus;
import net.sf.jabb.dstream.SimpleReceiveStatus;
import net.sf.jabb.dstream.StreamDataSupplier;
import net.sf.jabb.dstream.StreamDataSupplierWithId;
import net.sf.jabb.dstream.ex.DataStreamInfrastructureException;
import net.sf.jabb.util.attempt.AttemptStrategy;
import net.sf.jabb.util.attempt.StopStrategies;
import net.sf.jabb.util.ex.ExceptionUncheckUtility;
import net.sf.jabb.util.parallel.BackoffStrategies;
import net.sf.jabb.util.parallel.WaitStrategies;
import net.sf.jabb.util.parallel.WaitStrategy;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;

/**
 * StreamDataSupplier backed by AWS Kinesis.
 * One KinesisStreamDataSupplier instance covers only one shard of a Kinesis stream.
 * It supports user record de-aggregation. See http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-kpl-consumer-deaggregation.html
 * It defines position range as (startPosition, endPosition].
 * @author James Hu
 *
 */
public class KinesisStreamDataSupplier<M> implements StreamDataSupplier<M> {
	private static final Logger logger = LoggerFactory.getLogger(KinesisStreamDataSupplier.class);
	
	private static final int MAX_GET_RECORDS_LIMIT = 1000;	// per getRecords(...) as specified by AWS
	private static final long RETRY_INTERVAL_AFTER_THRESHOLD_EXCEEDED = 2000L; // 2 seconds
	private static final long DEFAULT_RETRY_INTERVAL_BASE = 1000L;
	private static final int LAST_POSITION_POLL_SECONDS = 3;	// number of seconds to wait for polling the last position
	
	protected Function<UserRecord, M> messageConverter;

	protected AmazonKinesisClient client;		// let's assume that it is thread safe
	protected String streamName;
	protected String shardId;
	
	protected long pollInterval;	// number of milliseconds to wait for data to be available before next poll from within fetch(...) and receive(...) methods
	protected int fetchBatchSize;	// the "limit" used in client.getRecords(...) from within fetch(...) methods
	protected int receiveBatchSize;	// the "limit" used in client.getRecords(...) from within receive(...) methods
	
	protected WaitStrategy waitStrategy = WaitStrategies.threadSleepStrategy();
	protected AttemptStrategy attemptStrategy = new AttemptStrategy()
		.withWaitStrategy(waitStrategy)
		.withBackoffStrategy(BackoffStrategies.fibonacciBackoff(DEFAULT_RETRY_INTERVAL_BASE, DEFAULT_RETRY_INTERVAL_BASE*5))
		.withStopStrategy(StopStrategies.stopAfterTotalDuration(Duration.ofMillis(DEFAULT_RETRY_INTERVAL_BASE*20)));
	
	public KinesisStreamDataSupplier(AmazonKinesisClient client, String streamName, String shardId, 
			Function<UserRecord, M> messageConverter, 
			long pollInterval, int fetchBatchSize, int receiveBatchSize){
		this.client = client;
		this.streamName = streamName;
		this.shardId = shardId;
		this.messageConverter = messageConverter;
		this.pollInterval = pollInterval;
		Validate.isTrue(fetchBatchSize <= MAX_GET_RECORDS_LIMIT, "fetchBatchSize should not be greater than %d: %d", MAX_GET_RECORDS_LIMIT, fetchBatchSize);
		this.fetchBatchSize = fetchBatchSize;
		Validate.isTrue(receiveBatchSize <= MAX_GET_RECORDS_LIMIT, "receiveBatchSize should not be greater than %d: %d", MAX_GET_RECORDS_LIMIT, receiveBatchSize);
		this.receiveBatchSize = receiveBatchSize;
	}
	
	static class Position{
		private String sequenceNumber;
		private long subSequenceNumber;
		private boolean isLastUserRecord;
		
		Position(String position){
			if (isBeforeTheVeryFirst(position)){
				
			}else{
				int i = position.indexOf('/');
				sequenceNumber = position.substring(0, i);
				if (position.charAt(position.length() - 1) == '/'){
					isLastUserRecord = true;
					subSequenceNumber = Long.parseLong(position.substring(i + 1, position.length() - 1));
				}else{
					subSequenceNumber = Long.parseLong(position.substring(i + 1));
				}
			}
		}
		
		static Position of(String position){
			return new Position(position);
		}
		
		@Override
		public String toString(){
			return toString(sequenceNumber, subSequenceNumber, isLastUserRecord);
		}
		
		static String toString(String sequenceNumber, long subSequenceNumber, boolean isLastUserRecord){
			if (isLastUserRecord){
				return sequenceNumber + "/" + subSequenceNumber + "/";
			}else{
				return sequenceNumber + "/" + subSequenceNumber;
			}
		}
		
		static String toString(Position position){
			return toString(position.sequenceNumber, position.subSequenceNumber, position.isLastUserRecord);
		}
		
		static String getSequenceNumber(String position){
			int i = position.indexOf('/');
			return position.substring(0, i);
		}
		
		static boolean isBeforeTheVeryFirst(String position){
			return position == null || position.length() == 0 || position.equals("-1");
		}
		
		public boolean isBeforeTheVeryFirst(){
			return sequenceNumber == null;
		}
		
		public BigInteger getSequenceNumberAsBigInteger(){
			return new BigInteger(sequenceNumber);
		}
		
		public String getSequenceNumber() {
			return sequenceNumber;
		}
		public long getSubSequenceNumber() {
			return subSequenceNumber;
		}
		public boolean isLastUserRecord() {
			return isLastUserRecord;
		}
	}
	
	/**
	 * Create StreamDataSupplierWithIds from a Kinesis stream. 
	 * @param <M>		type of the elements in the stream
	 * @param endpoint	The endpoint (ex: "kinesis.us-east-1.amazonaws.com") or a full URL, including the protocol (ex: "https://kinesis.us-east-1.amazonaws.com") 
	 * 					See http://docs.aws.amazon.com/general/latest/gr/rande.html#ak_region
	 * @param streamName	name of the Kinesis stream
	 * @param messageConverter		the message converter to transform Kinesis records into desired objects
	 * @param pollInterval 			number of milliseconds to wait for data to be available before next poll
	 * @param fetchBatchSize		the batch size for getting records from Kinesis from within fetch(...) methods
	 * @param receiveBatchSize		the batch size for getting records from Kinesis from within receive(...) methods
	 * @return	list of StreamDataSupplierWithIds covering all shards of the Kinesis stream.
	 */
	public static <M> List<StreamDataSupplierWithId<M>> create(String endpoint, String streamName, 
			Function<UserRecord, M> messageConverter,
			long pollInterval, int fetchBatchSize, int receiveBatchSize){
		return create(null, null, endpoint, streamName, messageConverter, pollInterval, fetchBatchSize, receiveBatchSize);
	}
	
	/**
	 * Create StreamDataSupplierWithIds from a Kinesis stream.
	 * @param <M>		type of the elements in the stream
	 * @param awsAccessKeyId	explicitly specified AWS access key id, or null if the default should be used
	 * @param awsSecretKey		explicitly specified AWS secret key, or null if the default should be used
	 * @param endpoint	The endpoint (ex: "kinesis.us-east-1.amazonaws.com") or a full URL, including the protocol (ex: "https://kinesis.us-east-1.amazonaws.com") 
	 * 					See http://docs.aws.amazon.com/general/latest/gr/rande.html#ak_region
	 * @param streamName	name of the Kinesis stream
	 * @param messageConverter		the message converter to transform Kinesis records into desired objects
	 * @param pollInterval 			number of milliseconds to wait for data to be available before next poll
	 * @param fetchBatchSize		the batch size for getting records from Kinesis from within fetch(...) methods
	 * @param receiveBatchSize		the batch size for getting records from Kinesis from within receive(...) methods
	 * @return	list of StreamDataSupplierWithIds covering all shards of the Kinesis stream.
	 */
	public static <M> List<StreamDataSupplierWithId<M>> create(String awsAccessKeyId, String awsSecretKey, 
			String endpoint, String streamName, 
			Function<UserRecord, M> messageConverter,
			long pollInterval, int fetchBatchSize, int receiveBatchSize){
		AmazonKinesisClient client;
		if (awsAccessKeyId != null && awsSecretKey != null){
			client = new AmazonKinesisClient(new BasicAWSCredentials(awsAccessKeyId, awsSecretKey));
		}else{
			client = new AmazonKinesisClient();
		}
		client.setEndpoint(endpoint);
		
		DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
		describeStreamRequest.setStreamName( streamName );
		List<Shard> shards = new ArrayList<>();
		String exclusiveStartShardId = null;
		do {
		    describeStreamRequest.setExclusiveStartShardId( exclusiveStartShardId );
		    DescribeStreamResult describeStreamResult = client.describeStream( describeStreamRequest );
		    shards.addAll( describeStreamResult.getStreamDescription().getShards() );
		    if (describeStreamResult.getStreamDescription().getHasMoreShards() && shards.size() > 0) {
		        exclusiveStartShardId = shards.get(shards.size() - 1).getShardId();
		    } else {
		        exclusiveStartShardId = null;
		    }
		} while ( exclusiveStartShardId != null );
		
		return shards.stream().map(shard->{
			String shardId = shard.getShardId();
			return new KinesisStreamDataSupplier<>(client, streamName, shardId, messageConverter, pollInterval, fetchBatchSize, receiveBatchSize)
					.withId(shardId);
		}).collect(Collectors.toList());
	}
	
	protected String streamNameAndShardId(){
		return streamName + "/" + shardId;
	}
	
	/**
	 * This method always returns "-1"
	 */
	@Override
	public String firstPosition() {
		return "-1";
	}

	@Override
	public String firstPosition(Instant enqueuedAfter, Duration waitForArrival) throws InterruptedException, DataStreamInfrastructureException {
		throw new UnsupportedOperationException("Seeking by enqueuedAfter is not supported by Kinesis");
	}
	
	/**
	 * Get one Kinesis stream record according to the shard iterator
	 * @param shardIterator		the shard iterator specifying the position of the record
	 * @param waitSeconds		number of seconds to wait for the record to be available
	 * @return		the record of null if no such record
	 * @throws DataStreamInfrastructureException	if Kinesis replies with any error message
	 */
	protected Record getOneRecord(String shardIterator, int waitSeconds) throws DataStreamInfrastructureException {
		GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
		getRecordsRequest.setShardIterator(shardIterator);
		getRecordsRequest.setLimit(1);
		
		return ExceptionUncheckUtility.getThrowingUnchecked(()->new AttemptStrategy(attemptStrategy)
				.retryIfException(ProvisionedThroughputExceededException.class)
				.callThrowingSuppressed(()->{
					int i = waitSeconds;
					do{
						GetRecordsResult getRecordsResult = client.getRecords(getRecordsRequest);
						List<Record> records = getRecordsResult.getRecords();
						if (records != null && records.size() > 0){
							return records.get(0);
						}else{
							String nextIterator = getRecordsResult.getNextShardIterator();
							if (nextIterator != null){
								getRecordsRequest.setShardIterator(nextIterator);
							}else{
								break;
							}
						}
					}while (i-- > 0);
					return null;
				}));
	}
	
	/**
	 * Get the user record in Kinesis stream record by subSequenceNumber
	 * @param record	the original Kinesis stream record
	 * @param subSequenceNumber		the sub-sequence-number
	 * @return	the user record or null if not found
	 */
	protected UserRecord getUserRecord(Record record, long subSequenceNumber){
		List<UserRecord> userRecords = UserRecord.deaggregate(Collections.singletonList(record));
		int i = (int) subSequenceNumber;
		if (i < userRecords.size()){
			UserRecord q = userRecords.get(i);
			if (q.getSubSequenceNumber() == subSequenceNumber){
				return q;
			}
		}
		for (UserRecord r: userRecords){
			if (r.getSubSequenceNumber() == subSequenceNumber){
				return r;
			}
		}
		return null;
	}

	@Override
	public String lastPosition() throws DataStreamInfrastructureException {
		return lastPosition(LAST_POSITION_POLL_SECONDS);
	}
	
	public String lastPosition(int maxPollSeconds) throws DataStreamInfrastructureException {
		String shardIterator = client.getShardIterator(streamName, shardId, ShardIteratorType.LATEST.name()).getShardIterator();
		if (shardIterator != null){
			Record record = getOneRecord(shardIterator, maxPollSeconds);
			if (record != null){
				List<UserRecord> userRecords = UserRecord.deaggregate(Collections.singletonList(record));
				return Position.toString(record.getSequenceNumber(), userRecords.get(userRecords.size() - 1).getSubSequenceNumber(), true) ;
			}else{
				String actualFirstPosition = actualFirstPosition();
				if (actualFirstPosition == null){
					return null;
				}else{
					throw new DataStreamInfrastructureException("No record had been received in the last " + LAST_POSITION_POLL_SECONDS + " seconds, the first position is: " + actualFirstPosition);
				}
			}
		}else{
			throw new DataStreamInfrastructureException("Failed to get shard iterator for " + streamNameAndShardId() + " at the end of the stream");
		}
	}
	
	/**
	 * Get the actual first position which is at trim_horizon
	 * @return	the actual first position, or null if there is no data in this shard
	 * @throws DataStreamInfrastructureException	if unable to get it from Kinesis
	 */
	public String actualFirstPosition() throws DataStreamInfrastructureException {
		String shardIterator = client.getShardIterator(streamName, shardId, ShardIteratorType.TRIM_HORIZON.name()).getShardIterator();
		if (shardIterator != null){
			Record record = getOneRecord(shardIterator, 0);
			if (record != null){
				List<UserRecord> userRecords = UserRecord.deaggregate(Collections.singletonList(record));
				return Position.toString(record.getSequenceNumber(), 0, userRecords.size() == 1) ;
			}else{
				return null;
			}
		}else{
			throw new DataStreamInfrastructureException("Failed to get shard iterator for " + streamNameAndShardId() + " at the start of the stream");
		}
	}


	@Override
	public Instant enqueuedTime(String position) throws DataStreamInfrastructureException {
		String shardIterator = client.getShardIterator(streamName, shardId, ShardIteratorType.AT_SEQUENCE_NUMBER.name(), Position.getSequenceNumber(position)).getShardIterator();
		if (shardIterator != null){
			Record record = getOneRecord(shardIterator, 0);	// all user records in this record share the same approximateArrivalTimestamp
			return record == null ? null : record.getApproximateArrivalTimestamp().toInstant();
		}else{
			throw new DataStreamInfrastructureException("Failed to get shard iterator for " + streamNameAndShardId() + " starting at " + position);
		}
	}

	@Override
	public String nextStartPosition(String previousEndPosition) {
		return previousEndPosition;
	}

	@Override
	public boolean isInRange(String position, String endPosition) {
		Validate.isTrue(position != null, "position cannot be null");
		if (endPosition == null){
			return true;
		}else{
			if (Position.isBeforeTheVeryFirst(position)){
				return true;
			}
			Position pos = Position.of(position);
			Position endPos = Position.of(endPosition);
			
			BigInteger sequenceNumber = pos.getSequenceNumberAsBigInteger();
			BigInteger endSequenceNumber = endPos.getSequenceNumberAsBigInteger();
			switch (sequenceNumber.compareTo(endSequenceNumber)){
				case -1:
					return true;
				case 1:
					return false;
				default: //case 0:
					return pos.getSubSequenceNumber() <= endPos.getSubSequenceNumber();
			}
		}
	}
	
	@Override
	public boolean isInRange(Instant enqueuedTime, Instant endEnqueuedTime) {
		Validate.isTrue(enqueuedTime != null, "enqueuedTime cannot be null");
		if (endEnqueuedTime == null){
			return true;
		}else{
			return !enqueuedTime.isAfter(endEnqueuedTime);
		}
	}
	
	/**
	 * Get shard iterator by start position
	 * @param startPosition		the start position, exclusive. If it is null or "" or "-1", that means the position before the first one
	 * @return				the iterator or null if there is no data available
	 * @throws DataStreamInfrastructureException	if Kinesis replies with any error message
	 */
	protected String getShardIterator(Position startPosition) throws DataStreamInfrastructureException{
		GetShardIteratorResult getShardIteratorResult;
		try{
			if (startPosition.isBeforeTheVeryFirst()){
				getShardIteratorResult = client.getShardIterator(streamName, shardId, ShardIteratorType.TRIM_HORIZON.name());
			}else{
				getShardIteratorResult = client.getShardIterator(streamName, shardId, 
						startPosition.isLastUserRecord() ? ShardIteratorType.AFTER_SEQUENCE_NUMBER.name() : ShardIteratorType.AT_SEQUENCE_NUMBER.name(), 
						startPosition.getSequenceNumber());
			}
		}catch(Exception e){
			throw new DataStreamInfrastructureException("Failed to get shard iterator for " + streamNameAndShardId() + " starting from " + startPosition, e);
		}
		return getShardIteratorResult.getShardIterator();
	}

	/**
	 * Fetch records
	 * @param list					the list that received data will be put into
	 * @param startPosition			the start position, exclusive
	 * @param inRangePredicate		in range checker
	 * @param maxItems				maximum number of records that can be returned
	 * @param timeoutMillis			maximum number of milliseconds for this operation
	 * @return						receive status
	 * @throws InterruptedException			if interrupted
	 * @throws DataStreamInfrastructureException	if exception happened in the infrastructure
	 */
	protected SimpleReceiveStatus fetch(List<? super M> list, String startPosition, Predicate<Record> inRangePredicate, int maxItems, long timeoutMillis)
			throws InterruptedException, DataStreamInfrastructureException {
		Position startPos = Position.of(startPosition);
		
		SimpleReceiveStatus status = new SimpleReceiveStatus();

		String shardIterator = getShardIterator(startPos);
		int limit = maxItems;
		
		GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
		long timeout = System.currentTimeMillis() + timeoutMillis;
		while (shardIterator != null && limit > 0 && System.currentTimeMillis() < timeout){
			getRecordsRequest.setShardIterator(shardIterator);
			getRecordsRequest.setLimit(limit > fetchBatchSize ? fetchBatchSize : limit);
			GetRecordsResult getRecordsResult;
			try{
				getRecordsResult = client.getRecords(getRecordsRequest);
			}catch(ProvisionedThroughputExceededException e){
				logger.debug("ProvisionedThroughputExceeded, will retry after " + RETRY_INTERVAL_AFTER_THRESHOLD_EXCEEDED + "ms");
				waitStrategy.await(RETRY_INTERVAL_AFTER_THRESHOLD_EXCEEDED);	// will retry later
				continue;
			}catch(Exception e){
				throw new DataStreamInfrastructureException("Failed to get records", e);
			}
			
			List<Record> resultRecords = getRecordsResult.getRecords();
			if (resultRecords != null && resultRecords.size() > 0){
				boolean isStartPositionClear = startPos.isBeforeTheVeryFirst() 	// should include every user record
						|| startPos.isLastUserRecord(); 	// no overlap
				for (Record resultRecord: resultRecords){
					boolean isInTheStartRecord = resultRecord.getSequenceNumber().equals(startPos.getSequenceNumber()); // not in the same kinesis stream record
					List<UserRecord> records = UserRecord.deaggregate(Collections.singletonList(resultRecord));
					for (int i = 0; i < records.size(); i ++){
						UserRecord record = records.get(i);
						if (isStartPositionClear || !isInTheStartRecord
								|| record.getSubSequenceNumber() > startPos.getSubSequenceNumber())	// after the sub sequence number
						{
							if (!inRangePredicate.test(record)){
								status.setOutOfRangeReached(true);
								return status;
							}
							list.add(messageConverter.apply(record));
							status.setLastPosition(Position.toString(record.getSequenceNumber(), record.getSubSequenceNumber(), 
									i == records.size() - 1));
							status.setLastEnqueuedTime(record.getApproximateArrivalTimestamp().toInstant());
							if (--limit <= 0){
								return status;
							}
						}
					}
				}
			}else{
				// wait a while before next poll
				waitStrategy.await(pollInterval);
			}
			shardIterator = getRecordsResult.getNextShardIterator();
		}
		
		return status;
	}

	@Override
	public ReceiveStatus fetch(List<? super M> list, String startPosition, String endPosition, int maxItems, Duration timeoutDuration)
			throws InterruptedException, DataStreamInfrastructureException {
		return fetch(list, startPosition, record->isInRange(record.getSequenceNumber(), endPosition), maxItems, timeoutDuration.toMillis());
	}

	@Override
	public ReceiveStatus fetch(List<? super M> list, Instant startEnqueuedTime, Instant endEnqueuedTime, int maxItems, Duration timeoutDuration)
			throws InterruptedException, DataStreamInfrastructureException {
		throw new UnsupportedOperationException("Fetching by startEnqueuedTime is not supported by Kinesis");
	}

	@Override
	public ReceiveStatus fetch(List<? super M> list, String startPosition, Instant endEnqueuedTime, int maxItems, Duration timeoutDuration)
			throws InterruptedException, DataStreamInfrastructureException {
		return fetch(list, startPosition, record->isInRange(record.getApproximateArrivalTimestamp().toInstant(), endEnqueuedTime), maxItems, timeoutDuration.toMillis());
	}

	@Override
	public String startAsyncReceiving(Consumer<M> receiver, String startPosition) throws DataStreamInfrastructureException {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public String startAsyncReceiving(Consumer<M> receiver, Instant startEnqueuedTime) throws DataStreamInfrastructureException {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public void stopAsyncReceiving(String id) {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	protected SimpleReceiveStatus receive(Function<M, Long> receiver, String startPosition, Predicate<Record> inRangePredicate) throws DataStreamInfrastructureException {
		Position startPos = Position.of(startPosition);

		SimpleReceiveStatus status = new SimpleReceiveStatus();

		String shardIterator = getShardIterator(startPos);
		
		try{
			GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
			long timeout = System.currentTimeMillis() + receiver.apply(null);
			while (shardIterator != null && System.currentTimeMillis() < timeout){
				getRecordsRequest.setShardIterator(shardIterator);
				getRecordsRequest.setLimit(receiveBatchSize);
				GetRecordsResult getRecordsResult;
				try{
					getRecordsResult = client.getRecords(getRecordsRequest);
				}catch(ProvisionedThroughputExceededException e){
					logger.debug("ProvisionedThroughputExceeded, will retry after " + RETRY_INTERVAL_AFTER_THRESHOLD_EXCEEDED + "ms");
					waitStrategy.await(RETRY_INTERVAL_AFTER_THRESHOLD_EXCEEDED);
					continue;	// will retry
				}catch(Exception e){
					throw new DataStreamInfrastructureException("Failed to get records from " + streamNameAndShardId() + " starting from " + startPosition, e);
				}
				
				List<Record> resultRecords = getRecordsResult.getRecords();
				if (resultRecords != null && resultRecords.size() > 0){
					boolean isStartPositionClear = startPos.isBeforeTheVeryFirst() 	// should include every user record
							|| startPos.isLastUserRecord(); 	// no overlap
					for (Record resultRecord: resultRecords){
						boolean isInTheStartRecord = resultRecord.getSequenceNumber().equals(startPos.getSequenceNumber()); // not in the same kinesis stream record
						List<UserRecord> records = UserRecord.deaggregate(Collections.singletonList(resultRecord));
						for (int i = 0; i < records.size(); i ++){
							UserRecord record = records.get(i);
							if (isStartPositionClear || !isInTheStartRecord
									|| record.getSubSequenceNumber() > startPos.getSubSequenceNumber())	// after the sub sequence number
							{
								if (!inRangePredicate.test(record)){
									status.setOutOfRangeReached(true);
									return status;
								}
								long remainingTime = receiver.apply(messageConverter.apply(record));
								status.setLastPosition(Position.toString(record.getSequenceNumber(), record.getSubSequenceNumber(), 
										i == records.size() - 1));
								status.setLastEnqueuedTime(record.getApproximateArrivalTimestamp().toInstant());
								if (remainingTime <= 0){
									return status;
								}else{
									timeout = System.currentTimeMillis() + remainingTime;
								}
							}
						}
					}
				}else{
					// wait a while before next poll
					waitStrategy.await(pollInterval);
				}
				shardIterator = getRecordsResult.getNextShardIterator();
			}
		}catch(InterruptedException e){
			throw new DataStreamInfrastructureException("Interrupted while receiving from " + streamNameAndShardId() + " starting from " + startPosition, e);
		}catch(DataStreamInfrastructureException e){
			throw e;
		}catch(Exception e){
			throw new DataStreamInfrastructureException("Failed to receive from " + streamNameAndShardId() + " starting from " + startPosition, e);
		}
		
		return status;
	}
	
	@Override
	public ReceiveStatus receive(Function<M, Long> receiver, String startPosition, String endPosition) throws DataStreamInfrastructureException {
		return receive(receiver, startPosition, record->isInRange(record.getSequenceNumber(), endPosition));
	}

	@Override
	public ReceiveStatus receive(Function<M, Long> receiver, Instant startEnqueuedTime, Instant endEnqueuedTime)
			throws DataStreamInfrastructureException {
		throw new UnsupportedOperationException("Receiving by startEnqueuedTime is not supported by Kinesis");
	}

	@Override
	public ReceiveStatus receive(Function<M, Long> receiver, String startPosition, Instant endEnqueuedTime) throws DataStreamInfrastructureException {
		return receive(receiver, startPosition, record->isInRange(record.getApproximateArrivalTimestamp().toInstant(), endEnqueuedTime));
	}
	


	@Override
	public void start() throws Exception {
	}

	@Override
	public void stop() throws Exception {
	}

}