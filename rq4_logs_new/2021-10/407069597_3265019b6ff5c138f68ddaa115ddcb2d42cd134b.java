package com.bithumb.interest.api;

import com.bithumb.coin.service.CoinService;
import com.bithumb.common.response.ApiResponse;
import com.bithumb.common.response.ErrorCode;
import com.bithumb.common.response.StatusCode;
import com.bithumb.common.response.SuccessCode;
import com.bithumb.interest.api.dto.InterestRequest;
import com.bithumb.interest.api.dto.InterestResponse;
import com.bithumb.interest.application.InterestServiceImpl;
import com.bithumb.interest.domain.Interest;
import io.swagger.annotations.Api;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;


@RequestMapping
@RequiredArgsConstructor
@RestController
@Api
@CrossOrigin(origins = "*", allowCredentials = "false")
public class InterestController {
    private final InterestServiceImpl interestService;
    private final CoinService coinService;

    @GetMapping("/interests/{user-id}")
    public ResponseEntity<?> getInterests(@PathVariable(value = "user-id") long userId){
        List<InterestResponse> interests = interestService.getInterests(userId);
        ApiResponse apiResponse = ApiResponse.responseData(StatusCode.SUCCESS,
                SuccessCode.INTEREST_FINDALL_SUCCESS.getMessage(),interests);
        return ResponseEntity.status(HttpStatus.OK).body(apiResponse);
    }

    @PostMapping("/interest/{user-id}")
    public ResponseEntity<?> createInterest(@PathVariable(value = "user-id") long userId, @RequestBody InterestRequest interestRequest) throws IOException {
        String symbol = interestRequest.getSymbol();
        InterestResponse interestResponse =interestService.createInterest(userId,symbol);
        ApiResponse apiResponse = ApiResponse.responseData(StatusCode.SUCCESS,
                SuccessCode.INTEREST_CREATE_SUCCESS.getMessage(),interestResponse);
        return ResponseEntity.status(HttpStatus.OK).body(apiResponse);
    }

    @DeleteMapping("/interest/{user-id}")
    public ResponseEntity<?> deleteInterest(@PathVariable(value = "user-id") long userId, @RequestBody InterestRequest interestRequest) {
        String symbol = interestRequest.getSymbol();
        interestService.deleteInterest(symbol,userId);
        ApiResponse apiResponse = ApiResponse.responseMessage(StatusCode.SUCCESS,
                SuccessCode.INTEREST_DELETE_SUCCESS.getMessage());
        return ResponseEntity.status(HttpStatus.OK).body(apiResponse);

    }
}