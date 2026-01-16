package com.github.nut077.springninja.entity;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.EntityListeners;
import javax.persistence.MappedSuperclass;
import javax.persistence.Version;
import java.time.OffsetDateTime;

@Getter
@Setter(AccessLevel.PROTECTED)
@MappedSuperclass
@EntityListeners(value = Listeners.class)
public abstract class Common {

  private OffsetDateTime createdDate;
  private OffsetDateTime updatedDate;

  @Version private int version;
}