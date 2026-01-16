package CentralPoint;

public class Conversation {
    private PairUser pairUser;
    private String text;

    public Conversation(PairUser pairUser, String text) {
        this.pairUser = pairUser;
        this.text = text;
    }
    
    public PairUser getPairUser() {
        return this.pairUser;
    }
    
    public String getText() {
        return this.text;
    }
    
}