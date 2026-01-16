package com.kodilla.testing.forum;

import java.util.Objects;

public class ForumComment {
    ForumPost forumPost;
    String commentBody;
    String author;

    public ForumComment(ForumPost forumPost, String commentBody, String author) {
        this.forumPost = forumPost;
        this.commentBody = commentBody;
        this.author = author;
    }

    public ForumPost getForumPost() {
        return forumPost;
    }

    public String getCommentBody() {
        return commentBody;
    }

    public String getAuthor() {
        return author;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ForumComment that = (ForumComment) o;
        return Objects.equals(forumPost, that.forumPost) &&
                Objects.equals(commentBody, that.commentBody) &&
                Objects.equals(author, that.author);
    }

    @Override
    public int hashCode() {
        return Objects.hash(forumPost, commentBody, author);
    }
}