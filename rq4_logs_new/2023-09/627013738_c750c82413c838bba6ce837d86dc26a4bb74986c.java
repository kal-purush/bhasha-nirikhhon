package org.lamisplus.biometric.domain.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class Deduplication {

    String patientId = "";
    LocalDate deduplicationDate = null;
    Integer matchCount = 0;
    Integer unMatchCount = 0;
    Integer baselineFingerCount = 0;
    Integer recaptureFingerCount = 0;
    Integer perfectMatchCount = 0;
    Integer imperfectMatchCount = 0;
    Map<String, String> details = new HashMap<>();

}