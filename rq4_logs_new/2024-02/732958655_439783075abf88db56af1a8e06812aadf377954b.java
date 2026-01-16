package com.challenger.fridge.service;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.*;

import com.challenger.fridge.dto.MemberInfoDto;
import com.challenger.fridge.dto.box.response.StorageBoxNameResponse;
import com.challenger.fridge.dto.sign.SignUpRequest;
import com.challenger.fridge.dto.storage.request.StorageSaveRequest;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

@SpringBootTest
@Transactional
class MemberServiceTest {

    @Autowired MemberService memberService;
    @Autowired SignService signService;
    @Autowired StorageService storageService;

    private static final String EMAIL = "jjw@test.com";
    private static final String PASSWORD = "1234";
    private static final String NAME = "jjw";
    private Long mainStorageId;
    private Long subStorageId;

    @BeforeEach
    void setUp() {
        signService.registerMember(new SignUpRequest(EMAIL, PASSWORD, NAME));
        mainStorageId = storageService.saveStorage(new StorageSaveRequest("메인저장소", 1L, 1L), EMAIL);
        subStorageId = storageService.saveStorage(new StorageSaveRequest("서브저장소", 1L, 1L), EMAIL);
    }

    @DisplayName("회원 정보 조회")
    @Test
    void memberInfo() {
        String email = EMAIL;

        MemberInfoDto memberInfo = memberService.findUserInfo(email);
        List<StorageBoxNameResponse> storageBoxes = memberInfo.getStorageBoxes();

        assertThat(memberInfo.getUsername()).isEqualTo("jjw");
        assertThat(memberInfo.getEmail()).isEqualTo("jjw@test.com");
        assertThat(memberInfo.getMainStorageId()).isEqualTo(mainStorageId);
        assertThat(memberInfo.getMainStorageName()).isEqualTo("메인저장소");
        assertThat(storageBoxes.size()).isEqualTo(2);
    }

}