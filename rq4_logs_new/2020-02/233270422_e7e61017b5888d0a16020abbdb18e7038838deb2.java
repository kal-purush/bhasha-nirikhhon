package com.vcode.entity;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.Date;

/**
 * @author moyee
 * @version 1.0.0
 * @Description 标签实体
 * @return Tag实例
 * @since 1.0.0
 */
@Document("contest")
public class Contest {
  @Id
  private ObjectId Id;

  @Field("name")
  private String name;

  @Field("always")
  private boolean always; // true => 永久开放；false => 限时开放；

  @Field("start_time")
  private Date startTime;

  @Field("end_time")
  private Date endTime;
}