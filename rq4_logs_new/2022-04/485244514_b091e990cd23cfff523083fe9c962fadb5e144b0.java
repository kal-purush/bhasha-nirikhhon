package com.example.blogit.comment;

import com.example.blogit.user.Users;

import javax.persistence.*;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table
public class Comment {
    @Id
    @GeneratedValue(
            strategy = GenerationType.SEQUENCE
    )
    private Long id;

    @NotNull
    private UUID uuid;

    @NotNull
    @Column(name = "author_id", nullable = false)
    private Long authorId;

    @ManyToOne(optional = false)
    @JoinColumn(name = "author_id", insertable = false, updatable = false)
    private Users author;

    @NotBlank
    private String message;

    private int upvotes;

    @NotNull
    private LocalDateTime createdAt;

    public Comment() {
        this.uuid = UUID.randomUUID();
        this.upvotes = 0;
        this.createdAt = LocalDateTime.now();
    }

    public Comment(Long id, Long authorId, String message) {
        this.uuid = UUID.randomUUID();
        this.upvotes = 0;
        this.createdAt = LocalDateTime.now();

        this.id = id;
        this.authorId = authorId;
        this.message = message;
    }

    public Long getId() {
        return id;
    }

    public UUID getUuid() {
        return uuid;
    }

    public Long getAuthorId() {
        return authorId;
    }

    public Users getAuthor() {
        return author;
    }

    public String getMessage() {
        return message;
    }

    public int getUpvotes() {
        return upvotes;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }
}