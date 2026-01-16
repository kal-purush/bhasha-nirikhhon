package RESTModel;

public class RequestMessage {
    private int id;
    private String content;
    
    public RequestMessage() {
        super();
    }
    
    public RequestMessage(int id, String content) {
        super();
        this.id = id;
        this.content = content;
    }
    
    public int getId() {
        return this.id;
    }
    
    public String getContent() {
        return this.content;
    }
    
    public void setId(int id) {
        this.id = id;
    }
    
    public void setContent(String content) {
        this.content = content;
    }
}