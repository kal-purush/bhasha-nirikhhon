package vn.edu.usth.outlook.sent;

// Message.java
public class Message {
    private String to;
    private String subject;
    private String content;
    private String timestamp;

    public Message(String to, String subject, String content, String timestamp) {
        this.to = to;
        this.subject = subject;
        this.content = content;
        this.timestamp = timestamp;
    }

    // Getters
    public String getTo() { return to; }
    public String getSubject() { return subject; }
    public String getContent() { return content; }
    public String getTimestamp() { return timestamp; }
}