package it.gov.pagopa.wispconverter.service;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import it.gov.pagopa.wispconverter.exception.AppErrorCodeMessageEnum;
import it.gov.pagopa.wispconverter.exception.AppException;
import it.gov.pagopa.wispconverter.repository.CacheRepository;
import it.gov.pagopa.wispconverter.repository.model.enumz.InternalStepStatus;
import it.gov.pagopa.wispconverter.service.model.ECommerceHangTimeoutMessage;
import it.gov.pagopa.wispconverter.service.model.re.ReEventDto;
import it.gov.pagopa.wispconverter.util.Constants;
import it.gov.pagopa.wispconverter.util.LogUtils;
import it.gov.pagopa.wispconverter.util.ReUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;

import static it.gov.pagopa.wispconverter.util.MDCUtil.setEcommerceHangTimerInfoInMDC;

@Service
@Slf4j
@RequiredArgsConstructor
public class ECommerceHangTimerService {


    @Value("${azure.sb.wisp-ecommerce-hang-timeout-queue.connectionString}")
    private String connectionString;

    @Value("${azure.sb.queue.ecommerce-hang-timeout.name}")
    private String queueName;

    @Value("wisp-converter.ecommerce-hang.timeout.seconds")
    private Integer expirationTime;

    @Autowired
    private final ReService reService;

    @Autowired
    private ServiceBusSenderClient serviceBusSenderClient;

    @Autowired
    private CacheRepository cacheRepository;

    @PostConstruct
    public void post() {
        serviceBusSenderClient = new ServiceBusClientBuilder()
                .connectionString(connectionString)
                .sender()
                .queueName(queueName)
                .buildClient();
    }

    /**
     * This method add a new scheduled message in the queue.
     * Note: This method use Redis Cache to handle the metadata of the message
     *
     * @param message the message to send on the queue of the service bus.
     */
    public void sendMessage(ECommerceHangTimeoutMessage message) {

        String noticeNumber = message.getNoticeNumber();
        String fiscalCode = message.getFiscalCode();
        setEcommerceHangTimerInfoInMDC(fiscalCode, noticeNumber);

        String key = String.format("wisp_timer_hang_%s_%s", noticeNumber, fiscalCode);

        // If the key is already present in the cache, we delete it to avoid duplicated message.
        if (cacheRepository.hasKey(key)) {
            cancelScheduledMessage(noticeNumber, fiscalCode);
        }

        // build the service bus message
        ServiceBusMessage serviceBusMessage = new ServiceBusMessage(message.toString());
        log.debug("Sending scheduled message {} to the queue: {}", message, queueName);

        // compute time and schedule message for consumer trigger
        OffsetDateTime scheduledExpirationTime = OffsetDateTime.now().plusSeconds(expirationTime);
        Long sequenceNumber = serviceBusSenderClient.scheduleMessage(serviceBusMessage, scheduledExpirationTime);

        // log event
        log.info("Sent scheduled message_base64 {} to the queue: {}", LogUtils.encodeToBase64(message.toString()), queueName);
        generateRE(InternalStepStatus.ECOMMERCE_HANG_TIMER_CREATED, "Scheduled ECommerceHangTimerService: [" + message + "]");

        // insert in Redis cache sequenceNumber of the message
        cacheRepository.insert(key, sequenceNumber.toString(), expirationTime);
    }


    /**
     * This method deletes a scheduled message from the queue
     *
     * @param noticeNumber use to find the message
     * @param fiscalCode   use to find the message
     */
    public void cancelScheduledMessage(String noticeNumber, String fiscalCode) {

        log.debug("Cancel scheduled message for ECommerceHangTimer {} {}", noticeNumber, fiscalCode);
        String key = String.format("wisp_timer_hang_%s_%s", noticeNumber, fiscalCode);

        // get the sequenceNumber from the Redis cache
        String sequenceNumber = cacheRepository.read(key, String.class);

        // cancel scheduled message
        if (sequenceNumber != null && this.callCancelScheduledMessage(sequenceNumber)) {
            log.info("Canceled scheduled message for ecommerce_hang_timeout_base64 {} {}", LogUtils.encodeToBase64(noticeNumber), LogUtils.encodeToBase64(fiscalCode));

            // delete the sequenceNumber from the Redis cache
            cacheRepository.delete(key);

            // log event
            log.debug("Deleted sequence number {} for ecommerce_hang_timeout_base64-token: {} {} from cache", sequenceNumber, noticeNumber, fiscalCode);
            generateRE(InternalStepStatus.ECOMMERCE_HANG_TIMER_DELETED, "Deleted sequence number: [" + sequenceNumber + "] for notice: [" + noticeNumber + "] for fiscalCode [" + fiscalCode + "]");
        }
    }

    private boolean callCancelScheduledMessage(String sequenceNumberString) {
        long sequenceNumber = Long.parseLong(sequenceNumberString);
        try {
            // delete the message from the queue
            serviceBusSenderClient.cancelScheduledMessage(sequenceNumber);
            return true;
        } catch (Exception exception) {
            throw new AppException(AppErrorCodeMessageEnum.PERSISTENCE_SERVICE_BUS_CANCEL_ERROR, exception.getMessage());
        }
    }


    private void generateRE(InternalStepStatus status, String otherInfo) {
        // setting data in MDC for next use
        // TODO fix the info
        ReEventDto reEvent = ReUtil.getREBuilder()
                .status(status)
                .domainId(MDC.get(Constants.MDC_DOMAIN_ID))
                .paymentToken(MDC.get(Constants.MDC_PAYMENT_TOKEN))
                .info(otherInfo)
                .build();
        reService.addRe(reEvent);
    }

}