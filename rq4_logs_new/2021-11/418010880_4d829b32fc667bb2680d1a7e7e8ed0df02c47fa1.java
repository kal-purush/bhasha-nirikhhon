package com.ibcs.admin_test2.api;

import com.ibcs.admin_test2.dto.ChangePasswordDto;
import com.ibcs.admin_test2.dto.LoginDto;
import com.ibcs.admin_test2.dto.RegistrationDto;
import com.ibcs.admin_test2.dto.ResponseDto;
import com.ibcs.admin_test2.service.AuthService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping("/auth")
public class AuthApi {

    @Autowired
    private AuthService authService;

    // consumes = {MediaType.APPLICATION_JSON_VALUE, MediaType.MULTIPART_FORM_DATA_VALUE}
    @PostMapping(path = "/img")
    public ResponseDto photoPost(@RequestPart("photo") MultipartFile photo) {
        return authService.photo( photo);
    }

    // consumes = {MediaType.APPLICATION_JSON_VALUE, MediaType.MULTIPART_FORM_DATA_VALUE}
    @PostMapping(path = "/registration")
    public ResponseDto registration(@RequestBody RegistrationDto aa) {
        return authService.registation(aa);
    }

    @PostMapping("/login")
    public ResponseDto login(@RequestBody LoginDto aa) {
        return authService.login(aa);
    }

    @PostMapping("/changePassword")
    public ResponseDto changePassword(@RequestBody ChangePasswordDto aa) {
        return authService.changePassword(aa);
    }

}