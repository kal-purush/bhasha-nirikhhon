package org.detective.controller.Reply;

import org.detective.dto.AssignedRequestDTO;
import org.detective.dto.EstimateDTO;
import org.detective.dto.EstimateDetailDTO;
import org.detective.dto.ReplyDTO;
import org.detective.services.Reply.ReplyService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/reply")
public class ReplyController {

    private ReplyService replyService;
    public ReplyController(ReplyService replyService) {
        this.replyService = replyService;
    }

    @PostMapping("")
    public ResponseEntity<String> ReplyController(@RequestBody ReplyDTO replyDTO) {
        System.err.println("ReplyController : createRequest 실행");
        System.err.println("ReplyController RequestDTO : "+replyDTO);
        try {
            replyService.createEstimate(replyDTO);
           return ResponseEntity.ok("Request Created");
       } catch (Exception e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        }
    }

    @GetMapping("/detail")
    public List<EstimateDetailDTO> getEstimateDetail(@RequestParam("requestId") Long requestId) {
        System.err.println("@@@@@@@@@@@@@@@@@@@@@@@@@@");
        return replyService.getEstimateDetail(requestId);
    }

    @GetMapping("/estimate")
    public List<EstimateDTO> getEstimateList(@RequestParam("email") String email) {
        return replyService.getEstimateList(email);
    }
}