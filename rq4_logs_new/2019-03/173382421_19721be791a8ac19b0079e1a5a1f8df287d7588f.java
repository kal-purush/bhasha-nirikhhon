package Service;

import Service.interfaces.IMessages;

public class DialogMessages implements IMessages {

    @Override
    public void ShowMessage(MessageType type, String message) {

    }

    @Override
    public void ShowMessage(MessageType type, String message, String title) {

    }

    @Override
    public void ShowMessage(MessageType type, String message, String title, String header) {

    }

    @Override
    public boolean ConfirmMessage(MessageType type, String message, String title, String header) {
        return false;
    }

}