package com.fpr;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

@Component
public class financialProductAdmin {
    @Value("${financial.api_key}")
    private String financialApiKey;
//    @Scheduled(cron = "0 0 0 * * *") //매일 0시 0분 0초에 실행예약
    @Scheduled(fixedDelay = 1000000)
    public void updateFinancialProduct () {
        RestTemplate rt = new RestTemplate();
        String url = String.format("https://finlife.fss.or.kr/finlifeapi/depositProductsSearch.json?auth=%s&topFinGrpNo=020000&pageNo=1", financialApiKey);
        ResponseEntity<String> response = rt.getForEntity(
                url,
                String.class
        );
        System.out.println(response);

        //TODO Json Parser 사용하여 DTO
    }


}