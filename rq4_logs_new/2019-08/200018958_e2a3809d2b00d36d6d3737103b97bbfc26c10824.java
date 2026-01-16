package uk.gov.hmcts.reform.workallocation.queue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.microsoft.azure.servicebus.IMessage;
import com.microsoft.azure.servicebus.IQueueClient;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import uk.gov.hmcts.reform.workallocation.model.Task;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class QueueProducerTest {

    private ObjectMapper mapper;
    private QueueProducer<Task> queueProducer;
    public List<IMessage> itemsToSend = new ArrayList<>();

    @Before
    public void setUp() {
        itemsToSend = new ArrayList<>();
        mapper = new ObjectMapper();
        mapper.registerModule(new Jdk8Module()).registerModule(new JavaTimeModule());
        QueueClientSupplier supplier = mock(QueueClientSupplier.class);
        doAnswer(invocation -> createClient()).when(supplier).get();
        queueProducer = new QueueProducer<>(supplier, mapper);
    }

    @Test
    public void testPlaceItemInQueue() {
        List<Task> tasks = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            tasks.add(Task.builder().id(Integer.toString(i)).build());
        }
        queueProducer.placeItemsInQueue(tasks, Task::getId);
        Assert.assertEquals(5, itemsToSend.size());
        Assert.assertEquals("0", itemsToSend.get(0).getMessageId());
        Assert.assertEquals("Task", itemsToSend.get(0).getLabel());
    }

    private IQueueClient createClient() throws ServiceBusException, InterruptedException {
        IQueueClient client = mock(IQueueClient.class);
        doAnswer(invocation -> {
            IMessage message = invocation.getArgument(0);
            itemsToSend.add(message);
            return null;
        }).when(client).send(any(IMessage.class));
        return client;
    }

    static class SendClient {
        public List<IMessage> itemsToSend = new ArrayList<>();

        public void send(IMessage message) {
            itemsToSend.add(message);
        }
    }
}