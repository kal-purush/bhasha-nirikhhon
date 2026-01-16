package com.likelion.hackathon.controller;

import org.springframework.web.bind.annotation.RestController;

import com.likelion.hackathon.domain.Habit;
import com.likelion.hackathon.domain.User;
import com.likelion.hackathon.dto.response.habit.EditHabitPageResponse;
import com.likelion.hackathon.dto.request.habit.CheckHabitRequest;
import com.likelion.hackathon.dto.request.habit.CreateHabitRequest;
import com.likelion.hackathon.dto.request.habit.EditHabitRequest;
import com.likelion.hackathon.dto.response.habit.MainPageResponse;
import com.likelion.hackathon.dto.response.habit.MainPageHabit;
import com.likelion.hackathon.service.HabitService;
import com.likelion.hackathon.service.UserService;

import lombok.RequiredArgsConstructor;

import java.time.LocalDate;
import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.GetMapping;

@RestController
@RequiredArgsConstructor
public class HabitController {
    private final UserService userService;
    private final HabitService habitService;

    @GetMapping("/home")
    public ResponseEntity getMainpageInfo() {
        String userId = SecurityContextHolder.getContext().getAuthentication().getName();
        List<MainPageHabit> result = habitService.getTodayHabits(userId);
        if (result == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("습관 정보가 없습니다.");
        }
        User user = (User) userService.loadUserByUsername(userId);
        return ResponseEntity.ok(new MainPageResponse(user.getImage(), result));
        // return new String();
    }
    
    @GetMapping("/home/{date}")
    public ResponseEntity getHabitsByDate(@PathVariable LocalDate date) {
        String userId = SecurityContextHolder.getContext().getAuthentication().getName();
        List<MainPageHabit> result = habitService.getHabitsByDate(userId, date);
        if (result == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("습관 정보가 없습니다.");
        }
        User user = (User) userService.loadUserByUsername(userId);
        return ResponseEntity.ok(new MainPageResponse(user.getImage(), result));
    }
    
    @PostMapping("/home/habit/create")
    public ResponseEntity createHabit(@RequestBody CreateHabitRequest dto) {
        String userId = SecurityContextHolder.getContext().getAuthentication().getName();
        habitService.createHabit(userId, dto);
        return ResponseEntity.status(HttpStatus.CREATED).body("습관이 추가되었습니다.");
    }
    
    @GetMapping("/home/habit/{habitId}")
    public ResponseEntity getEditHabitPage(@PathVariable int habitId) {
        String userId = SecurityContextHolder.getContext().getAuthentication().getName();
        EditHabitPageResponse result = habitService.getHabitById(userId, habitId);
        if (result == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("습관 정보가 없습니다.");
        }
        return ResponseEntity.ok(result);
    }

    @PutMapping("/home/habit/change/{habitId}")
    public ResponseEntity putMethodName(@PathVariable int habitId, @RequestBody EditHabitRequest dto) {
        String userId = SecurityContextHolder.getContext().getAuthentication().getName();
        Habit result = habitService.editHabit(userId, habitId, dto);
        if (result == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("오류가 발생하였습니다.");
        }
        return ResponseEntity.ok("습관 수정에 성공하였습니다.");
    }
    
    @DeleteMapping("/home/habit/delete/{habitId}")
    public ResponseEntity deleteHabit(@PathVariable int habitId) {
        String userId = SecurityContextHolder.getContext().getAuthentication().getName();
        habitService.deleteHabit(habitId);
        return ResponseEntity.ok("습관이 성공적으로 삭제되었습니다.");
    }
    
    @PutMapping("home/habit/check/{habitId}")
    public ResponseEntity putMethodName(@PathVariable int habitId, @RequestBody CheckHabitRequest dto) {
        String userId = SecurityContextHolder.getContext().getAuthentication().getName();
        habitService.checkHabit(userId, habitId, dto);
        return ResponseEntity.ok("습관 체크 성공");
    }
}