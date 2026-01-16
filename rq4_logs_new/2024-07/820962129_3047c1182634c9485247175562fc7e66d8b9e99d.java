package com.honestefforts.fixengine.model.message;

import com.honestefforts.fixengine.model.message.tags.RawTag;
import java.util.Map;
import lombok.Builder;

@Builder
public record FixMessageContext(Map<String, RawTag> processedMessages, int messageLength,
                                String messageType) {

}