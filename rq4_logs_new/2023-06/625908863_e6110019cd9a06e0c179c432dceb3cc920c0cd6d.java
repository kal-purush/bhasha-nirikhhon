package com.brainstrom.meokjang.notice.service;

import com.brainstrom.meokjang.chat.dto.ChatMessageDto;
import com.brainstrom.meokjang.food.domain.Food;
import com.brainstrom.meokjang.food.repository.FoodRepository;
import com.brainstrom.meokjang.food.service.FoodService;
import com.brainstrom.meokjang.notice.dto.FCMNotificationRequestDto;
import com.brainstrom.meokjang.user.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

@Service
public class SchedulerService {

    private final FCMNotificationService noticeService;
    private final UserService userService;
    private final FoodRepository foodRepo;

    @Autowired
    public SchedulerService(FCMNotificationService noticeService, UserService userService, FoodRepository foodRepo) {
        this.noticeService = noticeService;
        this.userService = userService;
        this.foodRepo = foodRepo;
    }

    @Scheduled(cron = "0 * * * * ?")
    public void run() {
        expireNotice();
    }

    public void expireNotice() {
        userService.getUserList().forEach(user -> {
            for(Food food:foodRepo.findAllByUserIdThanOrderByExpireDate(user.getUserId())) {
                if (food.getExpireDate().isBefore(LocalDate.now())) {
                    sendNotice(food, "유통기한이 지났습니다!");
                    break;
                } else if (food.getExpireDate().isBefore(LocalDate.now().plusDays(1))) {
                    sendNotice(food, "유통기한이 오늘까지에요!");
                    break;
                } else if (food.getExpireDate().isBefore(LocalDate.now().plusDays(3))) {
                    sendNotice(food, "유통기한이 3일 남았습니다!");
                    break;
                }
            }
        });
    }

    public void sendNotice(Food food, String message) {
        Map<String, String> data = new HashMap<>();
        data.put("type", "3");
        data.put("foodId", String.valueOf(food.getFoodId()));

        FCMNotificationRequestDto noticeRequestDto = FCMNotificationRequestDto.builder()
                .targetUserId(food.getUserId())
                .title("\u23F0 냉장고를 확인해보세요")
                .body(food.getFoodName()+"의 "+message)
                .data(data)
                .build();
        noticeService.sendNotificationByToken(noticeRequestDto);
    }
}