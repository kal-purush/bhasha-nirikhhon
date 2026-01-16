package com.revature.dtos;

import com.revature.models.PostType;

public class CommentPostRequest {
        private int userId;

        private String text;

        private int imageId;

        private PostType postType;

    public CommentPostRequest() {
    }

    public CommentPostRequest(int userId, String text, int imageId, PostType postType) {
        this.userId = userId;
        this.text = text;
        this.imageId = imageId;
        this.postType = postType;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public int getImageId() {
        return imageId;
    }

    public void setImageId(int imageId) {
        this.imageId = imageId;
    }

    public PostType getPostType() {
        return postType;
    }

    public void setPostType(PostType postType) {
        this.postType = postType;
    }
}