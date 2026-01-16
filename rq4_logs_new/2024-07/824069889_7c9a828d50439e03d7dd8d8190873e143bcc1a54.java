package cnsa.demo.controller;

import cnsa.demo.entity.Message;
import cnsa.demo.service.DemoService;
import com.mysql.cj.protocol.MessageSender;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Controller
public class DemoController {
    private final DemoService demoService;

    public DemoController(DemoService demoService) {
        this.demoService = demoService;
    }

    @GetMapping("/")
    public String getChatPage(Model model) {
        List<Message> messages = demoService.getAllMessage();
        model.addAttribute("messages", messages);
        return "index";
    }

    @PostMapping("/send")
    @ResponseBody
    public ResponseEntity<Map<String, String>> sendMessage(@RequestBody Map<String, String> messageText) {
        String userMessage = messageText.get("text");
        demoService.saveMessage(userMessage, true);

        List<Message> allMessages = demoService.getAllMessage();
        String gptResponse = demoService.getGptResponse(allMessages);
        demoService.saveMessage(gptResponse, false);

        Map<String, String> response = new HashMap<>();
        response.put("response", gptResponse);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/login")
    public String login() {
        return "login";
    }
}