package com.dominikp.mobileapp.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class UploadLocation {
    private Double latitude;
    private Double longitude;
    private String city;
    private String country;
}