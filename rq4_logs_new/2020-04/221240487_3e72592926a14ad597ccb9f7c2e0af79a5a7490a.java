package uk.gov.justice.hmpps.datacompliance.events.listeners;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.stereotype.Service;
import uk.gov.justice.hmpps.datacompliance.events.listeners.dto.OffenderDeletionCompleteEvent;
import uk.gov.justice.hmpps.datacompliance.events.listeners.dto.OffenderPendingDeletionEvent;
import uk.gov.justice.hmpps.datacompliance.events.listeners.dto.OffenderPendingDeletionReferralCompleteEvent;
import uk.gov.justice.hmpps.datacompliance.services.DeletionReferralService;

import java.io.IOException;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

@Slf4j
@Service
@ConditionalOnExpression("{'aws', 'localstack'}.contains('${inbound.referral.sqs.provider}')")
public class OffenderDeletionListener {

    private static final String OFFENDER_PENDING_DELETION_EVENT = "DATA_COMPLIANCE_OFFENDER-PENDING-DELETION";
    private static final String OFFENDER_PENDING_DELETION_REFERRAL_COMPLETE_EVENT = "DATA_COMPLIANCE_OFFENDER-PENDING-DELETION-REFERRAL-COMPLETE";
    private static final String OFFENDER_DELETION_COMPLETE_EVENT = "DATA_COMPLIANCE_OFFENDER-DELETION-COMPLETE";

    private final Map<String, MessageHandler> messageHandlers = Map.of(
            OFFENDER_PENDING_DELETION_EVENT, this::handlePendingDeletion,
            OFFENDER_PENDING_DELETION_REFERRAL_COMPLETE_EVENT, this::handleReferralComplete,
            OFFENDER_DELETION_COMPLETE_EVENT, this::handleDeletionComplete);

    private final ObjectMapper objectMapper;
    private DeletionReferralService deletionReferralService;

    public OffenderDeletionListener(final ObjectMapper objectMapper,
                                    final DeletionReferralService deletionReferralService) {

        log.info("Configured to listen to Offender Deletion events");

        this.objectMapper = objectMapper;
        this.deletionReferralService = deletionReferralService;
    }

    @JmsListener(destination = "${inbound.referral.sqs.queue.name}")
    public void handleEvent(final Message<String> message) {

        log.debug("Handling incoming offender deletion event: {}", message);

        messageHandlers.get(getEventType(message.getHeaders())).handle(message);
    }

    private String getEventType(final MessageHeaders messageHeaders) {

        final var eventType = messageHeaders.get("eventType", String.class);

        checkNotNull(eventType, "Message event type not found");
        checkState(messageHandlers.containsKey(eventType),
                "Unexpected message event type: '%s', expecting one of: %s", eventType, messageHandlers.keySet());

        return eventType;
    }

    private void handleDeletionComplete(final Message<String> message) {
        deletionReferralService.handleDeletionComplete(
                parseEvent(message.getPayload(), OffenderDeletionCompleteEvent.class));
    }

    private void handleReferralComplete(final Message<String> message) {
        deletionReferralService.handleReferralComplete(
                parseEvent(message.getPayload(), OffenderPendingDeletionReferralCompleteEvent.class));
    }

    private void handlePendingDeletion(final Message<String> message) {
        deletionReferralService.handlePendingDeletion(
                parseEvent(message.getPayload(), OffenderPendingDeletionEvent.class));
    }

    private <T> T parseEvent(final String requestJson, final Class<T> eventType) {
        try {
            return objectMapper.readValue(requestJson, eventType);
        } catch (final IOException e) {
            throw new RuntimeException("Failed to parse request: " + requestJson, e);
        }
    }

    @FunctionalInterface
    private interface MessageHandler {
        void handle(Message<String> message);
    }
}