package com.self.egoboard.domain.post.entity;

import com.self.egoboard.domain.post.entity.embedded.Board;
import com.self.egoboard.domain.user.entity.User;
import com.self.egoboard.global.Entity.BaseEntity;
import jakarta.persistence.AttributeOverride;
import jakarta.persistence.AttributeOverrides;
import jakarta.persistence.Column;
import jakarta.persistence.Embedded;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@Entity
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Table(name = "post")
public class Post extends BaseEntity {

  private static final int MAX_TITLE_LENGTH = 15;
  private static final int MAX_CONTENT_LENGTH = 512;
  private static final int MAX_FILENAME_LENGTH = 512;


  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "post_id", nullable = false, updatable = false)
  private Long id;

  @Enumerated(EnumType.STRING)
  private postCategory postCategory;

  @Embedded
  @AttributeOverrides({
      @AttributeOverride(name = "title", column = @Column(name = "title", nullable = false, length = MAX_TITLE_LENGTH)),
      @AttributeOverride(name = "content", column = @Column(name = "content", nullable = false, length = MAX_CONTENT_LENGTH)),
      @AttributeOverride(name = "uploadFileName", column = @Column(name = "upload_file_name", length = MAX_FILENAME_LENGTH)),
      @AttributeOverride(name = "storeFileName", column = @Column(name = "store_file_name", length = MAX_FILENAME_LENGTH))
  })
  private Board board;

  @ManyToOne(fetch = FetchType.LAZY)
  @JoinColumn(name = "user_id")
  private User user;

}