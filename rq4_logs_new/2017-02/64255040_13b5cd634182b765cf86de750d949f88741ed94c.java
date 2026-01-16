package org.dsa.iot.msiotdev.providers.iothub;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.dsa.iot.dslink.util.json.EncodingFormat;
import org.dsa.iot.dslink.util.json.JsonObject;
import org.dsa.iot.msiotdev.providers.MessageFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.PartitionReceiver;
import com.microsoft.azure.iothub.DeviceClient;
import com.microsoft.azure.iothub.Message;

public abstract class IotHubMessageFacade implements MessageFacade {
	private static final Logger LOG = LoggerFactory.getLogger(IotHubMessageFacade.class);

    protected DeviceClient device;
    private EventHubClient eventHubClient;
    private Consumer<JsonObject> consumer;
    private int partitionCount;
    private List<MessageHandler> messageHandlers;
    
    public IotHubMessageFacade(DeviceClient device, EventHubClient eventHubClient, int partitionCount) {
    	this.device = device;
        this.eventHubClient = eventHubClient;
        this.partitionCount = partitionCount;
        this.messageHandlers = new ArrayList<>();
    }
    
    public void init() {
    	
    	try {
            for (int i = 0; i < partitionCount; i++) {
                PartitionReceiver receiver = eventHubClient.createReceiverSync(
                        EventHubClient.DEFAULT_CONSUMER_GROUP_NAME,
                        String.valueOf(i),
                        Instant.now()
                );

                receiver.setReceiveTimeout(Duration.ofSeconds(1));
                MessageHandler handler = new MessageHandler(this, receiver);
                messageHandlers.add(handler);
                handler.schedule();
            }
        } catch (Exception e) {
            LOG.warn("Failed to setup partition receivers.", e);
        }
    }
    
    public abstract void setMessageProperty(Message msg);
    
    public abstract void tagMessage(JsonObject object);
    
    public abstract boolean shouldHandleEvent(JsonObject object);

    @Override
    public synchronized void emit(JsonObject object) {
    	tagMessage(object);
        Message msg = new Message(object.encode(EncodingFormat.MESSAGE_PACK));
        setMessageProperty(msg);
        device.sendEventAsync(msg, (responseStatus, callbackContext) -> {
        }, null);
    }

    @Override
    public void handle(Consumer<JsonObject> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void destroy() {
        for (MessageHandler handler : messageHandlers) {
            handler.disable();
        }

        messageHandlers.clear();

        if (device != null) {
            try {
                device.close();
            } catch (IOException ignored) {
            }
        }

        try {
            eventHubClient.close();
            eventHubClient = null;
        } catch (Exception e) {
            LOG.warn("Failed to destroy event hub client.", e);
        }

    }

    public Consumer<JsonObject> getConsumer() {
        return consumer;
    }

}