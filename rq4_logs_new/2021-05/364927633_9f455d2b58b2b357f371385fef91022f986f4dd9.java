package services.mesages;

import services.DBService.DBException;
import services.DBService.DBService;
import services.DBService.dataSets.MessageDataSet;

import java.util.List;

public class MessageService {
private final DBService dbService =new DBService();

    public void addMessage(String message, String messageTag, String login) throws DBException {
        dbService.addMessage(message, messageTag, login);
    }

    public List<MessageDataSet> getMessages() throws DBException {
        return dbService.getMessages();
    }

    public List<MessageDataSet> getMessagesByTag(String tag) throws DBException {
        return dbService.getMessagesByTag(tag);
    }
}