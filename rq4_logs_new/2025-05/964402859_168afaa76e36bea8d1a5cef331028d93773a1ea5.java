package com.fatepet.petrest.business.admin;

import com.fatepet.global.response.ApiResponse;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/admin")
public class AdminBusinessController {

    @GetMapping("/business/check-name?name={businessName}")
    public ResponseEntity<ApiResponse<Void>> checkBusinessName(@PathVariable("businessName") String businessName) {


    }
}