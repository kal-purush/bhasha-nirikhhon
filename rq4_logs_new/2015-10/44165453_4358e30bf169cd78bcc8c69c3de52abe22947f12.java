package CentralPoint;

public class FileAckInfo {
    private boolean isAck;
    private int port;  
    
    public FileAckInfo(boolean isAck) {
        this.isAck = isAck;
    }
    
    public FileAckInfo(boolean isAck, int port) {
        this.isAck = isAck;
        this.port = port;
    }
    
    public void setIsAck(boolean isAck) {
        this.isAck = isAck;
    }
    
    public void setPort(int port) {
        this.port = port;
    }

    public boolean getIsAck() {
        return this.isAck;
    }
    
    public int getPort() {
        return this.port;
    }
        
}