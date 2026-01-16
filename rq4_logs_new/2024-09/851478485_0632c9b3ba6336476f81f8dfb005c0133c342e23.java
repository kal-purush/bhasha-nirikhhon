package org.detective.controller.Estimate;

import org.detective.dto.EstimateDetailDTO;
import org.detective.dto.EstimateFormDTO;
import org.detective.dto.EstimateListDTO;
import org.detective.services.Estimate.EstimateService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
//@RequestMapping("/reply")
@RequestMapping("/estimate")
public class EstimateController {

    private final EstimateService estimateService;
    public EstimateController(EstimateService estimateService) {
        this.estimateService = estimateService;
    }

    @PostMapping("") // 고객의 의뢰에 대한 탐정의 답변서 작성 메서드
    public ResponseEntity<String> createEstimate(@RequestBody EstimateFormDTO estimateFormDTO) {
        System.err.println("ReplyController : createRequest 실행");
        System.err.println("ReplyController RequestDTO : "+ estimateFormDTO);
        try {
            estimateService.createEstimate(estimateFormDTO);
           return ResponseEntity.ok("Estimate Created");
       } catch (Exception e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        }
    }
    @GetMapping("/list") // 고객에게 보낸 탐정의 답변서 목록 조회 메서드
    public List<EstimateListDTO> getEstimateList(@RequestParam("userId") Long userId) {
        return estimateService.getEstimateList(userId);
    }

    @GetMapping("/details") // 고객이 보낸 의뢰애 대한 탐정의 답변서 상세정보 조회메서드
    public ResponseEntity<List<EstimateDetailDTO>> getEstimateDetail(@RequestParam("requestId") Long requestId,@RequestParam(value = "userId", required = false) Long userId) {
        return ResponseEntity.ok(estimateService.getEstimateDetail(requestId, userId));
    }

}