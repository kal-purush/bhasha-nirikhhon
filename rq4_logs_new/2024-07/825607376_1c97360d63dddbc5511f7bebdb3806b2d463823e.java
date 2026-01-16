package refooding.api.domain.exchange.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import refooding.api.domain.exchange.dto.RegionResponse;
import refooding.api.domain.exchange.service.RegionService;

import java.util.List;

@Tag(name = "식재료 교환 API")
@RestController
@RequestMapping("/regions")
@RequiredArgsConstructor
public class ExchangeController {

    private final RegionService regionService;

    @GetMapping
    @Operation(
            summary = "식재료 교환 지역 목록 조회",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "지역 목록 조회 성공",
                            content = @Content(schema = @Schema(implementation = RegionResponse.class))
                    )
            }
    )
    public ResponseEntity<List<RegionResponse>> regions() {
        List<RegionResponse> response = regionService.getRegionsWithChildren();
        return ResponseEntity.ok(response);
    }
}