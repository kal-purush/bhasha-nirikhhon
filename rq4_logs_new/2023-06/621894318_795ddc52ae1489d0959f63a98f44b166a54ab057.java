package com.gxdxx.instagram.service.user;

import com.gxdxx.instagram.dto.request.UserSignUpRequest;
import com.gxdxx.instagram.dto.response.UserSignUpResponse;
import com.gxdxx.instagram.entity.User;
import com.gxdxx.instagram.exception.NicknameAlreadyExistsException;
import com.gxdxx.instagram.repository.UserRepository;
import com.gxdxx.instagram.service.S3Uploader;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.security.crypto.password.PasswordEncoder;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class UserCreateServiceTest {

    @InjectMocks
    private UserCreateService userCreateService;
    @Mock
    private UserRepository userRepository;
    @Mock
    private S3Uploader s3Uploader;
    @Mock
    private PasswordEncoder passwordEncoder;

    @Test
    @DisplayName("[회원가입] - 성공")
    void saveUser_withValidRequest_shouldSaveUser() {
        // given
        String nickname = "nickname";
        String password = "password";
        String encodedPassword = "encodedPassword";
        String storedFileName = "storedFileName";
        MockMultipartFile mockFile = getMockMultipartFile();
        UserSignUpRequest request = new UserSignUpRequest(nickname, password, mockFile);
        when(userRepository.findByNickname(anyString())).thenReturn(Optional.empty());
        when(s3Uploader.upload(any(), anyString())).thenReturn(storedFileName);
        when(passwordEncoder.encode(anyString())).thenReturn(encodedPassword);
        when(userRepository.save(any(User.class))).thenReturn(User.of(nickname, encodedPassword, storedFileName));

        // when
        UserSignUpResponse response = userCreateService.createUser(request);

        // then
        assertEquals(nickname, response.nickname());
        assertEquals(storedFileName, response.profileImageUrl());
    }

    @Test
    @DisplayName("[회원가입] - 실패 (이미 존재하는 닉네임)")
    void saveUser_withExistingNickname_shouldThrowException() {
        // given
        String existingNickname = "existingNickname";
        String password = "password";
        String encodedPassword = "encodedPassword";
        String storedFileName = "storedFileName";
        MockMultipartFile mockFile = getMockMultipartFile();
        UserSignUpRequest request = new UserSignUpRequest(existingNickname, password, mockFile);
        User existingUser = User.of(existingNickname, encodedPassword, storedFileName);
        when(userRepository.findByNickname(existingNickname)).thenReturn(Optional.of(existingUser));

        // when & then
        assertThrows(NicknameAlreadyExistsException.class, () -> userCreateService.createUser(request));
    }

    private MockMultipartFile getMockMultipartFile() {
        MockMultipartFile mockFile = new MockMultipartFile(
                "file",
                "test.png",
                "image/png",
                "test content".getBytes()
        );
        return mockFile;
    }

}