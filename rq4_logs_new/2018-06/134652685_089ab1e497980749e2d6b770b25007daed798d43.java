package ca.huynhat.itsasteal.models;

public class Comment {
    private String comment_id;
    private String user_id;
    private String content;

    public Comment(String comment_id, String user_id, String content) {
        this.comment_id = comment_id;
        this.user_id = user_id;
        this.content = content;
    }

    public String getComment_id() {
        return comment_id;
    }

    public String getUser_id() {
        return user_id;
    }

    public String getContent() {
        return content;
    }
}