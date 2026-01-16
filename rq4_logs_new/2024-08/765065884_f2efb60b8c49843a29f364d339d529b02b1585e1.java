package it.gov.pagopa.wispconverter.service;

import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import it.gov.pagopa.wispconverter.repository.CacheRepository;
import it.gov.pagopa.wispconverter.service.model.ECommerceHangTimeoutMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ECommerceHangTimerServiceTest {

    public static final String NOTICE_NUMBER = "23456789";
    public static final String FISCAL_CODE = "ASDAS1212";
    public static final Long SEQUENCE_NUMBER = 1111L;


    @Mock
    private ReService reService;

    @Mock
    private ServiceBusSenderClient serviceBusSenderClient;

    @Mock
    private CacheRepository cacheRepository;

    @InjectMocks
    @Spy
    private ECommerceHangTimerService eCommerceHangTimerService;

    @Captor
    ArgumentCaptor<ServiceBusMessage> messageArgumentCaptor;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(eCommerceHangTimerService, "connectionString", "-");
        ReflectionTestUtils.setField(eCommerceHangTimerService, "queueName", "your-queue-name");
        ReflectionTestUtils.setField(eCommerceHangTimerService, "serviceBusSenderClient", serviceBusSenderClient);
        ReflectionTestUtils.setField(eCommerceHangTimerService, "expirationTime", 1800);
        ReflectionTestUtils.setField(eCommerceHangTimerService, "reService", reService);
        ReflectionTestUtils.setField(eCommerceHangTimerService, "cacheRepository", cacheRepository);
    }

    @Test
    void sendMessage_ok() {
        ECommerceHangTimeoutMessage message = ECommerceHangTimeoutMessage.builder()
                .noticeNumber(NOTICE_NUMBER)
                .fiscalCode(FISCAL_CODE)
                .build();
        eCommerceHangTimerService.sendMessage(message);
        verify(cacheRepository, times(1)).hasKey(eq("wisp_timer_hang_" + NOTICE_NUMBER + "_" + FISCAL_CODE));
        verify(serviceBusSenderClient, times(1)).scheduleMessage(messageArgumentCaptor.capture(), any());
        assertEquals("{\"fiscalCode\":\"" + FISCAL_CODE + "\",\"noticeNumber\":\"" + NOTICE_NUMBER + "\"}",
                messageArgumentCaptor.getValue().getBody().toString());
    }

    @Test
    void sendMessage_messageDuplicated() {
        ECommerceHangTimeoutMessage message = ECommerceHangTimeoutMessage.builder()
                .noticeNumber(NOTICE_NUMBER)
                .fiscalCode(FISCAL_CODE)
                .build();
        when(cacheRepository.hasKey(anyString())).thenReturn(Boolean.TRUE);
        when(cacheRepository.read(any(), any())).thenReturn(SEQUENCE_NUMBER.toString());

        eCommerceHangTimerService.sendMessage(message);

        verify(cacheRepository, times(1)).delete(eq("wisp_timer_hang_" + NOTICE_NUMBER + "_" + FISCAL_CODE));
        verify(serviceBusSenderClient, times(1)).cancelScheduledMessage(SEQUENCE_NUMBER);
        verify(cacheRepository, times(1)).hasKey(eq("wisp_timer_hang_" + NOTICE_NUMBER + "_" + FISCAL_CODE));
        verify(serviceBusSenderClient, times(1)).scheduleMessage(messageArgumentCaptor.capture(), any());
    }

    @Test
    void cancelScheduledMessage_ok() {
        when(cacheRepository.read(any(), any())).thenReturn(SEQUENCE_NUMBER.toString());

        eCommerceHangTimerService.cancelScheduledMessage(NOTICE_NUMBER, FISCAL_CODE);

        verify(cacheRepository, times(1)).delete(eq("wisp_timer_hang_" + NOTICE_NUMBER + "_" + FISCAL_CODE));
        verify(serviceBusSenderClient, times(1)).cancelScheduledMessage(SEQUENCE_NUMBER);

    }
}