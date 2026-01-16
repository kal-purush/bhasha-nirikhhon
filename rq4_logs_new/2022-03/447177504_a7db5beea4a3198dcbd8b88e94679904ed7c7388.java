public class Message {
    String sender, receiver, type, msg;
    byte[] buffer;
    int chunkSize;
    public Message(String type, String sender, String receiver, String msg){
        this.type = type;
        this.sender = sender;
        this.receiver = receiver;
        this.msg = msg;
    }
    public Message(String type, String sender, String receiver, byte[] buffer, int chunkSize){
        this.type = type;
        this.sender = sender;
        this.receiver = receiver;
        this.buffer = buffer;
        this.chunkSize = chunkSize;
    }
}