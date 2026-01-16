package refooding.api.domain.exchange.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import refooding.api.domain.exchange.dto.request.ExchangeCreateRequest;
import refooding.api.domain.exchange.dto.request.ExchangeUpdateRequest;
import refooding.api.domain.exchange.dto.response.ExchangeDetailResponse;
import refooding.api.domain.exchange.dto.response.RegionResponse;

import java.util.List;

@Tag(name = "식재료 교환 API")
public interface ExchangeControllerOpenApi {

    @Operation(
            summary = "식재료 교환 상세 조회",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "식재료 교환 상세 조회 성공"
                    )
            }
    )
    @Parameter(
            name = "exchangeId",
            description = "게시글 아이디",
            required = true
    )
    ResponseEntity<ExchangeDetailResponse> getExchangeById(@PathVariable Long exchangeId);

    @Operation(
            summary = "식재료 교환글 등록",
            responses = {
                    @ApiResponse(
                            responseCode = "201",
                            description = "식재료 교환글 등록 성공"
                    )
            }
    )
    ResponseEntity<Void> create(@RequestBody ExchangeCreateRequest request);

    @Operation(
            summary = "식재료 교환글 수정",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "식재료 교환글 수정 성공"
                    )
            }
    )
    @Parameter(
            name = "exchangeId",
            description = "게시글 아이디",
            required = true
    )
    ResponseEntity<Void> update(@PathVariable Long exchangeId, @RequestBody ExchangeUpdateRequest request);

    @Operation(
            summary = "식재료 교환글 삭제",
            responses = {
                    @ApiResponse(
                            responseCode = "204",
                            description = "식재료 교환글 삭제 성공"
                    )
            }
    )
    @Parameter(
            name = "exchangeId",
            description = "게시글 아이디",
            required = true
    )
    ResponseEntity<Void> delete(@PathVariable Long exchangeId);

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
    ResponseEntity<List<RegionResponse>> regions();
}