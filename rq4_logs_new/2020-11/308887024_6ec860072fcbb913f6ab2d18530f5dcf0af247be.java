package com.marketplace.eventstore.framework.event;

import org.immutables.value.Value;

import java.util.UUID;

@Value.Immutable
public abstract class EventRecord {
  /** aggregateId */
  public abstract String getId();

  /** id of the event */
  public abstract UUID getEventId();

  /** the type of event or the className of the event */
  public abstract String getEventType();

  public abstract Event getEvent();

  public abstract int getVersion();

  public static EventRecord fromEvent(String streamId, Event event, int version) {
    return ImmutableEventRecord.builder()
        .id(streamId)
        .event(event)
        .eventId(event.getId())
        .eventType(event.getClass().getCanonicalName())
        .version(version)
        .build();
  }
}