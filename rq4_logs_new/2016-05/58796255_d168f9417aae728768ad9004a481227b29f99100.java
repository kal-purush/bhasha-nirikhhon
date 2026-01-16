package by.aleks.geekschat;

/**
 * Created by Alex on 5/15/16.
 */
public class Message {

    private User user;
    private String content;
    private long timeMs;

    public Message(User user, String content, long timeMs) {
        this.user = user;
        this.content = content;
        this.timeMs = timeMs;
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public long getTimeMs() {
        return timeMs;
    }

    public void setTimeMs(long timeMs) {
        this.timeMs = timeMs;
    }
}