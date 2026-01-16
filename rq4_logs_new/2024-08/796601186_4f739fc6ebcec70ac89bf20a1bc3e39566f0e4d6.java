package com.everycare.backend.domain.chatbot.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class MedicationStatisticsResponse {
    private String statistics;
}