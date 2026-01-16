package com.example.EnjoyTripBackend.util;

import com.example.EnjoyTripBackend.domain.Airplane;
import com.example.EnjoyTripBackend.dto.airplane.AirplaneResponseDto;
import com.example.EnjoyTripBackend.exception.EnjoyTripException;
import com.example.EnjoyTripBackend.exception.ErrorCode;
import com.example.EnjoyTripBackend.service.AirplaneService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Component
@RequiredArgsConstructor
public class AirplaneInfoCollector {

    @Value("${flight.service.key}")
    private String serviceKey;

    private final AirplaneService airplaneService;
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;

    @Scheduled(cron = "0 48 19 * * *") // 매일 오후 7시 48분에 실행
    public void fetchFlightInfo() {
        LocalDate today = LocalDate.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");

        List<String> depAirportIds = List.of("NAARKJB", "NAARKJJ", "NAARKJK", "NAARKJY", "NAARKNW", "NAARKNY", "NAARKPC", "NAARKPK", "NAARKPS", "NAARKPU", "NAARKSI", "NAARKSS", "NAARKTH", "NAARKTN", "NAARKTU");
        List<String> arrAirportIds = List.of("NAARKJB", "NAARKJJ", "NAARKJK", "NAARKJY", "NAARKNW", "NAARKNY", "NAARKPC", "NAARKPK", "NAARKPS", "NAARKPU", "NAARKSI", "NAARKSS", "NAARKTH", "NAARKTN", "NAARKTU");
        List<String> airlineIds = List.of("AAR", "ABL", "ASV", "ESR", "FGW", "HGG", "JJA", "JNA", "KAL", "TWB");

        for (int i = 0; i < 31; i++) { // Collect data for 31 days
            LocalDate date = today.plusDays(i);
            String depPlandTime = date.format(formatter);

            for (String depAirportId : depAirportIds) {
                for (String arrAirportId : arrAirportIds) {
                    if (!depAirportId.equals(arrAirportId)) { // Ensure departure and arrival airports are different
                        for (String airlineId : airlineIds) {
                            fetchAndSaveFlightsForDate(depAirportId, arrAirportId, depPlandTime, airlineId);
                        }
                    }
                }
            }
        }
    }

    private void fetchAndSaveFlightsForDate(String depAirportId, String arrAirportId, String depPlandTime, String airlineId) {
        String url = String.format("http://apis.data.go.kr/1613000/DmstcFlightNvgInfoService/getFlightOpratInfoList?serviceKey=%s&_type=json&depAirportId=%s&arrAirportId=%s&depPlandTime=%s&airlineId=%s",
                serviceKey, depAirportId, arrAirportId, depPlandTime, airlineId);

        ResponseEntity<String> responseEntity = restTemplate.getForEntity(url, String.class);

        if (responseEntity.getStatusCode().is2xxSuccessful()) {
            String responseBody = responseEntity.getBody();

            try {
                JsonNode rootNode = objectMapper.readTree(responseBody);
                JsonNode itemsNode = rootNode.path("response").path("body").path("items").path("item");

                if (itemsNode.isArray()) {
                    for (JsonNode itemNode : itemsNode) {
                        String airlineNm = getSafeText(itemNode, "airlineNm");
                        String arrAirportNm = getSafeText(itemNode, "arrAirportNm");
                        String depAirportNm = getSafeText(itemNode, "depAirportNm");
                        String depPlandTimeStr = getSafeText(itemNode, "depPlandTime");
                        String arrPlandTimeStr = getSafeText(itemNode, "arrPlandTime");
                        String economyCharge = getSafeText(itemNode, "economyCharge");
                        String prestigeCharge = getSafeText(itemNode, "prestigeCharge");
                        String vihicleId = getSafeText(itemNode, "vihicleId");

                        if (arrAirportNm != null && depAirportNm != null && !arrAirportNm.equals(depAirportNm)) {
                            AirplaneResponseDto airplaneDto = new AirplaneResponseDto();
                            airplaneDto.setAirlineNm(airlineNm);
                            airplaneDto.setArrAirportNm(arrAirportNm);
                            airplaneDto.setDepAirportNm(depAirportNm);
                            airplaneDto.setDepPlandTime(depPlandTimeStr);
                            airplaneDto.setArrPlandTime(arrPlandTimeStr);
                            airplaneDto.setEconomyCharge(economyCharge);
                            airplaneDto.setPrestigeCharge(prestigeCharge);
                            airplaneDto.setVihicleId(vihicleId);

                            airplaneService.save(airplaneDto);
                        }
                    }
                }
            } catch (Exception e) {
                throw new EnjoyTripException(ErrorCode.FAIL_INSERT_AIRPLANE_DATA, e.getMessage());
            }
        } else {
            throw new EnjoyTripException(ErrorCode.FAIL_INSERT_AIRPLANE_DATA, "Failed to fetch airplane data from the API");
        }
    }

    private String getSafeText(JsonNode node, String key) {
        JsonNode valueNode = node.get(key);
        return valueNode != null ? valueNode.asText() : null;
    }
}