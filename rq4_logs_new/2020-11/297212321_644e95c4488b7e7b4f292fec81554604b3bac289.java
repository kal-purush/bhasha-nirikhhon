package uno.csci4830.mavitapi.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import uno.csci4830.mavitapi.payload.request.thread.CreateThreadRequest;
import uno.csci4830.mavitapi.payload.request.thread.EditThreadRequest;
import uno.csci4830.mavitapi.service.ThreadService;

import static uno.csci4830.mavitapi.AuthUtils.canEdit;

@Controller
@RestController("/api/thread")
public class ThreadController {

    @Autowired
    private ThreadService threadService;

    SecurityContextHolder securityContextHolder;

    @PostMapping("/create")
    public ResponseEntity<?> create(@RequestBody CreateThreadRequest createThreadRequest) {
        try {
            return ResponseEntity.ok().body(threadService.createThread(createThreadRequest));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        }
    }

    @PostMapping("/edit")
    public ResponseEntity<?> edit(@RequestBody EditThreadRequest editThreadRequest) {
        try {
            if (canEdit()) {
                return ResponseEntity.ok().body(threadService.editThread(editThreadRequest));
            } else {
                return ResponseEntity.status(403).body("User does not have permission to edit threads");
            }
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        }
    }

//    @PostMapping("/delete")
//    public ResponseEntity<?> delete(@RequestBody String deleteThreadRequest) {
//        try {
//            if (canEdit()) {
//
//            }
//        }
//    }

    @GetMapping("/getThreads")
    public ResponseEntity<?> getThreads() {
        try {
            return ResponseEntity.ok().body(threadService.getAllThreads());
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        }
    }




}