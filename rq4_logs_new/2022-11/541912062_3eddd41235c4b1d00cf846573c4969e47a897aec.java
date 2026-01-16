package shop.kokodo.calculateservice.controller;

import io.restassured.RestAssured;
import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import shop.kokodo.calculateservice.DocumentConfiguration;
import shop.kokodo.calculateservice.dto.SaleListSearchCondition;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.restdocs.restassured3.RestAssuredRestDocumentation.document;

/**
 * packageName    : shop.kokodo.calculateservice.controller
 * fileName       : CommissionControllerTest
 * author         : namhyeop
 * date           : 2022/11/04
 * description    :
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2022/11/04        namhyeop       최초 생성
 */

class CommissionControllerTest extends DocumentConfiguration {

    @DisplayName("매출내역 리스트 조회")
    @Test
    public void searchSaleList() throws Exception{
        //given
        Long sellerId = 1L;
        LocalDateTime startDate = LocalDateTime.parse("1999-01-01T00:00:00");
        LocalDateTime endDate = LocalDateTime.parse("2999-01-01T00:00:00");
        String searchCondition = null;

        SaleListSearchCondition saleListSearchCondition = new SaleListSearchCondition(sellerId, startDate, endDate, searchCondition);
        //when
        final ExtractableResponse<Response> response = RestAssured
                .given(spec).log().all()
                .filter(document("calculate-commission-saleList"))
                .body(saleListSearchCondition)
                .contentType(MediaType.APPLICATION_JSON_VALUE)
                .when().post("/commission/saleList")
                .then().log().all().extract();
        //then
        assertThat(response.statusCode()).isEqualTo(HttpStatus.OK.value());
    }
}