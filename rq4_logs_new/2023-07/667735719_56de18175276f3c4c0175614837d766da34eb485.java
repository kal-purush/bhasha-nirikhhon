package com.example.youtubeClone.controller;

import com.example.youtubeClone.dto.Reply;
import com.example.youtubeClone.service.ReplyService;
import jakarta.websocket.server.PathParam;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;

@Controller
@RequiredArgsConstructor
@RequestMapping("/reply")
public class ReplyController {

    private final ReplyService replyService;

    @PostMapping("/save")
    public String replySave(@RequestBody ReplyForm replyForm) {
        Reply reply = new Reply();
        reply.setContent(replyForm.getContent());
        reply.setUsername(replyForm.getUsername());
        reply.setBoardId(replyForm.getBoardId());
        reply.setDateTime(LocalDateTime.now().toString());
        replyService.addReply(replyForm.getParentId(), reply);

        return "redirect:/";
    }

    @PostMapping("/update/{id}")
    public String replyUpdate(@PathVariable Long id, @RequestBody ReplyForm replyForm) {
        Reply reply = replyService.findOne(id);

        reply.setContent(replyForm.getContent());
        reply.setDateTime(LocalDateTime.now().toString());
        replyService.updateReply(id, reply);

        return "redirect:/";
    }

    @DeleteMapping("/delete/{id}")
    public String replyDelete(@PathVariable Long id) {
        replyService.deleteReply(id);

        return "redirect:/";
    }
}