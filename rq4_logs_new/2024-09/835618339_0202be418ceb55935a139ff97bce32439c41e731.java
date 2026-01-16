package com.kh.erp.service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestParam;

import com.kh.erp.dao.TbEmpDao;
import com.kh.erp.dto.TbEmpDto;

@Service
public class DateService {
    
    @Autowired
    private TbEmpDao tbEmpDao;
    
    public long dateChange(@RequestParam String loginId) {
        TbEmpDto tbEmpDto = tbEmpDao.selectOne(loginId);
        // 문자열을 LocalDateTime으로 변환하기 위한 포맷터 정의
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        try {
            // 문자열을 LocalDateTime으로 변환
            LocalDateTime localDateTime = LocalDateTime.parse(tbEmpDto.getEmpSdate(), formatter);

            // LocalDateTime을 LocalDate로 변환
            LocalDate sDate = localDateTime.toLocalDate();

            // 오늘 날짜(LocalDate) 구하기
            LocalDate today = LocalDate.now();

            // Sdate와 오늘 날짜 사이의 차이 계산
            long daysBetween = ChronoUnit.DAYS.between(sDate, today);

            return daysBetween;
        } catch (DateTimeParseException e) {
            e.printStackTrace();
            return -1; // 에러 발생 시 -1 반환
        }
    }
}