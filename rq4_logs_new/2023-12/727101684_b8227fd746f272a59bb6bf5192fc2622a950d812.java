package com.example.demo.applianceapi.api;

import com.example.demo.applianceapi.dto.ApplianceDTO;
import com.example.demo.applianceapi.service.ApplianceService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@Slf4j
@RequiredArgsConstructor
@RequestMapping("/appliance")
@CrossOrigin
public class ApplianceController {

    private final ApplianceService applianceService;

    @GetMapping(path = "/{pageNum}", produces = "application/json; charset=UTF-8")
    public ResponseEntity<List<ApplianceDTO>> getAppliances(@PathVariable int pageNum) {
        List<ApplianceDTO> appliances = applianceService.getAppliances(pageNum);

        return new ResponseEntity<>(appliances, HttpStatus.OK);
    }

}