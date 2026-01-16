package it.gov.pagopa.wispconverter.servicebus;

import com.azure.core.util.BinaryData;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceivedMessageContext;
import it.gov.pagopa.wispconverter.service.ReService;
import it.gov.pagopa.wispconverter.service.ReceiptService;
import it.gov.pagopa.wispconverter.service.model.ECommerceHangTimeoutMessage;
import it.gov.pagopa.wispconverter.service.model.ReceiptDto;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith(MockitoExtension.class)
class ECommerceHangTimeoutConsumerTest {
    public static final String NOTICE_NUMBER = "23456789";
    public static final String FISCAL_CODE = "ASDAS1212";


    @Mock
    ServiceBusReceivedMessageContext context;

    @Mock
    ServiceBusReceivedMessage message;

    @Mock
    ReService reService;

    @Mock
    ReceiptService receiptService;

    @InjectMocks
    @Spy
    private ECommerceHangTimeoutConsumer eCommerceHangTimeoutConsumer;

    @BeforeEach
    void setUp() {
    }

    @Captor
    ArgumentCaptor<List<ReceiptDto>> listArgumentCaptor;

    @Test
    void processMessage() {
        var dataMessage = ECommerceHangTimeoutMessage.builder()
                .fiscalCode(FISCAL_CODE)
                .noticeNumber(NOTICE_NUMBER)
                .build();

        Mockito.when(message.getBody()).thenReturn(BinaryData.fromObject(dataMessage));
        Mockito.when(context.getMessage()).thenReturn(message);

        eCommerceHangTimeoutConsumer.processMessage(context);

        Mockito.verify(receiptService, Mockito.times(1)).sendKoPaaInviaRtToCreditorInstitution(listArgumentCaptor.capture());
        assertEquals(1, listArgumentCaptor.getValue().size());
        assertEquals(FISCAL_CODE, listArgumentCaptor.getValue().get(0).getFiscalCode());
        assertEquals(NOTICE_NUMBER, listArgumentCaptor.getValue().get(0).getNoticeNumber());
        assertNull(listArgumentCaptor.getValue().get(0).getPaymentToken());
    }
}