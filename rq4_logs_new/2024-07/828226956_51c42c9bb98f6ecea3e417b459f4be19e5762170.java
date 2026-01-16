package adhd.diary.chatgpt.controller;

import adhd.diary.chatgpt.dto.ChatMessage;
import adhd.diary.chatgpt.service.ChatGptService;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ChatGptController {

    private final ChatGptService chatGptService;

    public ChatGptController(ChatGptService chatGptService) {
        this.chatGptService = chatGptService;
    }

    @PostMapping("/chatgpt/diary-analyze")
    public CompletableFuture<ResponseEntity<String>> createChatCompletion(@RequestParam String diaryContents) {
        return chatGptService.analyzeDiaryContents(List.of(new ChatMessage(diaryContents)))
                .thenApply(ResponseEntity::ok)
                .exceptionally(e -> ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error : " + e.getMessage()));
    }
}