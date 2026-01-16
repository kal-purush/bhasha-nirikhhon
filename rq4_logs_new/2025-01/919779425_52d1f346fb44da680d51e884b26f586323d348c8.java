package com.aivle.TermCompass.controller;

import com.aivle.TermCompass.domain.Record;
import com.aivle.TermCompass.domain.User;
import com.aivle.TermCompass.dto.RecordRequestDto;
import com.aivle.TermCompass.repository.UserRepository;
import com.aivle.TermCompass.service.RecordService;
import com.aivle.TermCompass.service.RequestService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

import java.util.List;
import java.util.Optional;

@RequiredArgsConstructor
@Controller
public class RecordController {
    private final RecordService recordService;
    private final RequestService requestService;
    private final UserRepository userRepository;

    @PostMapping("/terms")
    public ResponseEntity<Object> createTermsRecord(@RequestBody RecordRequestDto recordRequestDto) {
        Optional<User> optionalUser = userRepository.findById(recordRequestDto.getUserId());
        if (optionalUser.isEmpty()) {
            return ResponseEntity.badRequest().body("User not found.");
        }

        User user = optionalUser.get();

        Record record = recordService.createRecord(user, recordRequestDto.getRecordType(), recordRequestDto.getResult());
        recordService.addRequest(record, recordRequestDto.getRequest(), recordRequestDto.getFile(), recordRequestDto.getAnswer());

        return ResponseEntity.ok(record);
    }

    @PostMapping("/chat")
    public ResponseEntity<Object> createChatRecord(@RequestBody RecordRequestDto recordRequestDto) {
        Optional<User> optionalUser = userRepository.findById(recordRequestDto.getUserId());
        if (optionalUser.isEmpty()) {
            return ResponseEntity.badRequest().body("User not found.");
        }

        User user = optionalUser.get();

        Record record = recordService.createRecord(user, recordRequestDto.getRecordType(), recordRequestDto.getResult());
        recordService.addRequest(record, recordRequestDto.getRequest(), null, recordRequestDto.getAnswer());

        return ResponseEntity.ok(record);
    }

    @GetMapping("/records/{userId}")
    public ResponseEntity<List<Record>> getRecordsByUser(@PathVariable Long userId) {
        Optional<User> optionalUser = userRepository.findById(userId);
        if (optionalUser.isEmpty()) {
            return ResponseEntity.badRequest().body(null);
        }

        User user = optionalUser.get();

        return ResponseEntity.ok(recordService.getRecordsByUser(user));
    }
}