package com.everycare.backend.domain.medicinerecord.controller;

import com.everycare.backend.domain.medicinerecord.dto.DrugDetails;
import com.everycare.backend.domain.medicinerecord.dto.DrugInfoDetails;
import com.everycare.backend.domain.medicinerecord.service.MedicineRecordService;
import com.everycare.backend.global.common.RestApiResponse;
import com.everycare.backend.global.exception.BusinessException;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

import static com.everycare.backend.global.common.SuccessCode.FIND_DRUG_INFO_SUCCESS;
import static com.everycare.backend.global.common.SuccessCode.FIND_DRUG_SUCCESS;

@RestController
@Tag(name = "MedicineInfo API", description = "의약품 검색 및 상세 정보 조회 API")
@RequestMapping("/api/v1/medicines")
@RequiredArgsConstructor
public class MedicineInfoController {

    @Autowired
    private MedicineRecordService medicineRecordService;

    @GetMapping(value = "/findName", produces = "application/json")
    @Operation(summary = "의약품 검색 API", description = " '타이'를 검색하면 해당하는 단어가 전부 들어간 의약품 이름 리스트를 전부 전송")
    public ResponseEntity<RestApiResponse> getDrugNames(@RequestParam String drugName) {
        List<String> drugNames =  medicineRecordService.findDrugNames(drugName);
        return ResponseEntity.ok(RestApiResponse.of(FIND_DRUG_SUCCESS, drugNames));
    }

    @GetMapping(value = "/find-drug-info", produces = "application/json")
    @Operation(summary = "의약품 검색 (부가 설명 포함) API", description = " '타이'를 검색하면 해당하는 단어가 전부 들어간 의약품 정보 리스트(사진, 이름, 주성분, 회사, 구분)를 전부 전송")
    public ResponseEntity<RestApiResponse> getDrugInfos(@RequestParam String drugName) {
        List<DrugInfoDetails> drugInfos =  medicineRecordService.findDrugInfos(drugName);
        return ResponseEntity.ok(RestApiResponse.of(FIND_DRUG_INFO_SUCCESS, drugInfos));
    }

    @ExceptionHandler(BusinessException.class)
    public ResponseEntity<?> handleDrugNotFoundException(BusinessException ex) {
        return ResponseEntity.status(ex.getErrorCode().getStatus())
                .body(RestApiResponse.of(ex.getErrorCode()));
    }

    @GetMapping(value = "/details", produces = "application/json")
    @Operation(summary = "의약품 상세정보 조회 API", description = "사용자가 선택한 의약품의 상세정보 조회 API")
    public DrugDetails getDrugDetails(@RequestParam String drugName) {
        return medicineRecordService.findDrugDetailsByName(drugName);
    }

}