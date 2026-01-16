package com.boot.Security;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.web.authentication.AuthenticationSuccessHandler;
import org.springframework.stereotype.Component;

import com.boot.Service.AttendenceService;
import com.boot.Service.MembershipService;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class CustomLoginSuccessHandler implements AuthenticationSuccessHandler {

	@Autowired
	private AttendenceService attendanceService;
	
	@Autowired
	private MembershipService memService;

    @Override
    public void onAuthenticationSuccess(HttpServletRequest request, HttpServletResponse response, Authentication authentication) throws IOException {
        log.info("Authentication success. User: {}", authentication.getName());
        
        try {
            // 로그인 성공 후 포인트 지급(서비스단에서 로그인uuid와 오늘날짜 확인해서 지급)
            attendanceService.checkAttendance();//출석기록테이블 정보 저장
            memService.updateMembership();//포인트지급(pthisttb, point, mileage, grade 업데이트)
            log.info("Attendance checked successfully for user: {}", authentication.getName());
        } catch (Exception e) {
            log.error("Error occurred while checking attendance", e);
        }

        // 리디렉션 처리 (로그인 성공 후 리디렉션 URL 처리)
        String redirectUrl = request.getParameter("redirect");
        if (redirectUrl == null || redirectUrl.isEmpty()) {
            redirectUrl = "/main"; // 기본 리디렉션 URL
        }
        log.info("Redirecting to: {}", redirectUrl);
        response.sendRedirect(redirectUrl);
    }
}