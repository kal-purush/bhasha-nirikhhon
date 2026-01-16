package com.example.EnjoyTripBackend.dto.airplane;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.*;

@Getter
@Builder
@ToString
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class AirplaneItem {
    private String airlineNm;
    private String arrAirportNm;
    private String arrPlandTime;
    private String depAirportNm;
    private String depPlandTime;
    private String economyCharge;
    private String prestigeCharge;
    private String vihicleId;
}