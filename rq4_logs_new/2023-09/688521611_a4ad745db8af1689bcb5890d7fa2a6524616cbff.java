package com.example.frontendjavafx.controller;

import com.example.frontendjavafx.model.ChatMessage;
import com.example.frontendjavafx.model.ChatSession;
import com.example.frontendjavafx.security.AppUser;
import com.example.frontendjavafx.security.AuthenticationService;
import com.example.frontendjavafx.service.ChatViewService;
import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.ListView;
import javafx.scene.control.TextArea;
import javafx.scene.text.Text;

import java.util.ArrayList;
import java.util.List;

public class ChatViewController {

    @FXML
    private ListView<String> sessionList_LV;

    @FXML
    private Text welcomeText_T;

    @FXML
    private Button logoutButton_B;

    @FXML
    private ListView<String> chatContentList_LV;

    @FXML
    private TextArea sendChatContent_TV;

    @FXML
    private Button sendButton_B;

    public void initialize(){

        List<AppUser> myContacts = ChatViewService.getInstance().getMyContacts();
        List<String> namesOfMyContacts = new ArrayList<>();

        for (AppUser contact : myContacts){
            namesOfMyContacts.add(contact.userName());
        }

        sessionList_LV.getItems().addAll(namesOfMyContacts);

        if (myContacts.size() > 0)
        {

            ChatSession firstChatSession = ChatViewService
                    .getInstance()
                    .findOrAddChatSessionByParticipantIds(
                            ChatViewService.getInstance().getMeAsUserObject().userId(),
                            myContacts.get(0).userId()
                    );

            for (ChatMessage msg : firstChatSession.chat_messages()){
                chatContentList_LV.getItems().add(msg.senderId() + ": " + msg.content());
            }
        }

    }

}