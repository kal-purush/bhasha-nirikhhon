package MyRemote;

import java.rmi.Remote;
import java.util.List;

public interface ChatInterface extends Remote {
//    String Person (String a ) throws Exception;
//    void Chat(String b) throws Exception;
    List<String> SendMessage(int id, String message)  throws Exception;
    List <String> GetStringChat() throws Exception;
    List<Message> GetChat()  throws Exception;
}