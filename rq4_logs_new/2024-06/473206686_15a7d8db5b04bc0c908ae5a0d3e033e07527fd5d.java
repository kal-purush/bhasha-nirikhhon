package se.sundsvall.messaging;

public final class Constants {

	private Constants() {}

	public static final String STATISTICS_PATH = "/statistics";
	public static final String CONVERSATION_HISTORY_PATH = "/conversation-history/{partyId}";
	public static final String STATISTICS_FOR_DEPARTMENTS_PATH = STATISTICS_PATH + "/departments";
	public static final String STATISTICS_FOR_SPECIFIC_DEPARTMENT_PATH = STATISTICS_FOR_DEPARTMENTS_PATH + "/{department}";
	public static final String BATCH_STATUS_PATH = "/status/batch/{batchId}";
	public static final String MESSAGE_STATUS_PATH = "/status/message/{messageId}";
	public static final String MESSAGE_AND_DELIVERY_PATH = "/message/{messageId}";
	public static final String DELIVERY_STATUS_PATH = "/status/delivery/{deliveryId}";
}