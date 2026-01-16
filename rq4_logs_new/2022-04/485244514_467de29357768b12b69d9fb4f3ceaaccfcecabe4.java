package com.example.blogit.blog;

import com.example.blogit.topic.Topic;

import javax.persistence.*;

@Entity
@Table
public class BlogTopicLinktable {
    @Id
    @GeneratedValue(
            strategy = GenerationType.IDENTITY
    )
    private Long id;

    @Column(insertable = false, updatable = false)
    private Long blog_id;

    @Column(insertable = false, updatable = false)
    private Long topic_id;

    @ManyToOne
    @JoinColumn(name = "blog_id")
    private Blog blog;

    @ManyToOne
    @JoinColumn(name = "topic_id")
    private Topic topic;

    public Topic getTopic() {
        return topic;
    }

    public Blog getBlog() {
        return blog;
    }

    public BlogTopicLinktable() {

    }

    public Long getId() {
        return id;
    }

    public Long getBlogId() {
        return blog_id;
    }

    public Long getTopicId() {
        return topic_id;
    }
}