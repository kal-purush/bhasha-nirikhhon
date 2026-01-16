package com.umc.accountbook.controller;

import com.umc.accountbook.service.HistoryService;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import java.text.SimpleDateFormat;
import java.util.Date;

@RestController
@RequiredArgsConstructor
@RequestMapping("history")
public class HistoryController {
    private final HistoryService historyService;

    @GetMapping("/saving-amount/{user_id}")
    public Long getSavingAmount(@PathVariable int user_id, @RequestParam @DateTimeFormat(pattern="yyyy-MM-dd") Date date){
        Long savingAmount = historyService.getSavingAmount(user_id,date);

        return savingAmount;
    }
}