package com.interview.entity;

import java.io.Serializable;

/**
 * 面试题目实体类.
 *
 * @author rxliuli
 */
public class Topic implements Serializable {
  /**
   * 问题的 id.
   */
  private Long id;

  /**
   * 题目类型 id.
   */
  private Long typeId;

  /**
   * 题目的标题.
   */
  private String title;

  /**
   * 题目的参考答案.
   */
  private String referenceAnswer;

  public Topic() {
  }

  public Topic(Long id, Long typeId, String title, String referenceAnswer) {

    this.id = id;
    this.typeId = typeId;
    this.title = title;
    this.referenceAnswer = referenceAnswer;
  }
}