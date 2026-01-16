package com.evtimov.landlordapp.landlordspring.chat;

import com.evtimov.landlordapp.landlordspring.models.Message;
import com.evtimov.landlordapp.landlordspring.models.User;
import com.evtimov.landlordapp.landlordspring.services.UserService;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;

@RestController
@RequestMapping(value = "/api/messages")
public class ChatController {
    private final ChatService chatService;
    private final UserService userService;

    private JSONObject messageJSON = new JSONObject();

    @Autowired
    public ChatController(ChatService chatService, UserService userService) {
        this.chatService = chatService;
        this.userService = userService;
    }

    @RequestMapping(method = RequestMethod.GET)
    public void getMessage(@RequestBody JSONObject messageDTO) {
        Message message = new Message();
        messageJSON = messageDTO;
        message.setText(messageDTO.getString("body"));
        message.setTimestamp((Date) messageDTO.get("date"));
        message.setSender(findSender(messageDTO));
        chatService.saveMessage(message);
    }

    private User findSender(JSONObject messageDTO) {
        String regToken = messageDTO.getString("token");
        return userService.findByRegToken(regToken);
    }

}