package com.example.greeting.api;

/**
 * Created by kiki on 4/27/17.
 */

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.Value;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = GreetingEvent.GreetingMessageChanged.class, name = "greeting-message-changed")
})
public interface GreetingEvent {

    String getId();

    @Value
    final class GreetingMessageChanged implements GreetingEvent {

        public final String id;
        public final String message;

        @JsonCreator
        public GreetingMessageChanged(String name, String message) {
            this.id = Preconditions.checkNotNull(name, "id");
            this.message = Preconditions.checkNotNull(message, "message");
        }

        @Override
        public String getId() {
            return this.id;
        }
    }


}
