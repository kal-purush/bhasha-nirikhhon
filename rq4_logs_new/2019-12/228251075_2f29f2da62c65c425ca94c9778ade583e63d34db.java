package com.example.pagination.model;

import lombok.*;

@Data
@Builder
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class FilterDto {

    private Integer pipeId;

    private String pipeStatus;

    private String pipeCreated;

    private Integer markingId;

    private String markingStatus;
}