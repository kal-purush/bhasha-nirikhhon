package com.marketplace.eventstore.impl.fixtures.classifiedad;

import com.marketplace.eventstore.framework.event.Event;

import java.util.UUID;

public class ClassifiedAdCreated implements Event {
  private final UUID id;
  private final UUID owner;
  private final UUID aggregateId;

  public ClassifiedAdCreated(UUID owner, UUID aggregateId) {
    this(UUID.randomUUID(), owner, aggregateId);
  }

  public ClassifiedAdCreated(String owner, String aggregateId) {
    this(UUID.randomUUID(), UUID.fromString(owner), UUID.fromString(aggregateId));
  }

  public ClassifiedAdCreated(UUID id, UUID owner, UUID aggregateId) {
    this.id = id;
    this.owner = owner;
    this.aggregateId = aggregateId;
  }

  public UUID getOwner() {
    return owner;
  }

  @Override
  public UUID getId() {
    return id;
  }

  @Override
  public UUID aggregateId() {
    return aggregateId;
  }
}