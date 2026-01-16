package com.boot.Controller;

import java.util.HashMap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.boot.DTO.UsertbDTO;
import com.boot.Security.CustomUserDetails;
import com.boot.Service.UserService_4;

import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
@RequestMapping("/api/user")
public class UserRestController {
	
	@Autowired
	private UserService_4 userService;
	
	
		
	// 이메일 수정
    @PatchMapping("/email")
//    public ResponseEntity<String> updateEmail(@RequestBody UsertbDTO user, Authentication authentication) {
    public ResponseEntity<String> updateEmail(@RequestBody UsertbDTO user) {
//        CustomUserDetails  userDetails = (CustomUserDetails) authentication.getPrincipal();//인증된 사용자 정보 가져옴
//        String currentUserId = userDetails.getUserId();//로그인 된 사용자 아이디
    	log.info("@# updateEmail로 접근");

        // 서비스에 UserDTO와 Authentication 정보를 넘김
        userService.updateEmail(user);

        return ResponseEntity.ok("이메일이 성공적으로 변경되었습니다.");
    }
    
    // 비번 수정
    @PatchMapping("/password")
    public ResponseEntity<String> updatePassword(@RequestBody UsertbDTO user) {
    	log.info("@# updatePassword로 접근");
    	userService.updatePassword(user);
    	
    	return ResponseEntity.ok("비밀번호가 성공적으로 변경되었습니다.");
    }
    
    // 핸드폰번호 수정
    @PatchMapping("/phone")
    public ResponseEntity<String> updatePhone(@RequestBody UsertbDTO user) {
    	log.info("@# updatePhone로 접근");
    	userService.updatePhone(user);
    	
    	return ResponseEntity.ok("휴대폰번호가 성공적으로 변경되었습니다.");
    }
    
    // 생년월일 수정
    @PatchMapping("/birth")
    public ResponseEntity<String> updateBirth(@RequestBody UsertbDTO user) {
    	log.info("@# updateBirth로 접근");
    	userService.updateBirth(user);
    	
    	return ResponseEntity.ok("생년월일이 성공적으로 변경되었습니다.");
    }
    
    // 환불계좌 수정
    @PatchMapping("/account")
    public ResponseEntity<String> updateAccount(@RequestBody UsertbDTO user) {
    	log.info("@# updateAccount로 접근");
    	userService.updateAccount(user);
    	
    	return ResponseEntity.ok("환불계좌가 성공적으로 변경되었습니다.");
    }
    
    // 선호장르 수정
    @PatchMapping("/genre")
    public ResponseEntity<String> updateSelectGenre(@RequestBody HashMap<String, String> param) {
    	log.info("@# updateSelectGenre로 접근");

		// JSON에서 "genreList"라는 key로 넘어온 값을 처리
		String genreList = param.get("genreList");
		
		// 서비스에 전달
		userService.updateSelectGenre(genreList);
		
		return ResponseEntity.ok("선호장르가 성공적으로 변경되었습니다.");
    }



}