package com.example.ged.src.oauth;

import com.example.ged.config.BaseException;
import com.example.ged.config.BaseResponse;
import com.example.ged.src.category.models.dto.GetUserJobCategoryRes;
import com.example.ged.src.oauth.models.PostLoginReq;
import com.example.ged.src.oauth.models.PostLoginRes;
import io.swagger.v3.oas.annotations.Operation;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

import static com.example.ged.config.BaseResponseStatus.SUCCESS;

@RestController
@RequiredArgsConstructor
public class AuthController {

    private final AuthService authService;

    /**
     * 2021-05-16 로그인 API
     * [POST] /sing-in
     * @param postLoginReq
     * @return
     */
    @ResponseBody
    @PostMapping("/sign-in")
    @Operation(summary = "로그인 API")
    public BaseResponse<PostLoginRes> signIn(@RequestBody PostLoginReq postLoginReq){
        try{
            PostLoginRes postLoginRes = authService.loginService(postLoginReq);
            return new BaseResponse<>(SUCCESS,postLoginRes);
        }catch(BaseException exception){
            return new BaseResponse<>(exception.getStatus());
        }
    }
}