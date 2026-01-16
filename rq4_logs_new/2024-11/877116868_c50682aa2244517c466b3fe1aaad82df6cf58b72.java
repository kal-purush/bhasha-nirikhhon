package com.eng.dto;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateSerializer;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDate;

@Getter
@NoArgsConstructor
public class StudyDto {
    private Long userId;
    private Long wordId;
    private Long meaningId;

    @JsonSerialize(using = LocalDateSerializer.class)
    @JsonDeserialize(using = LocalDateDeserializer.class)
    private LocalDate date;

    @Builder
    public StudyDto(Long userId, Long wordId, Long meaningId, LocalDate date) {
        this.userId = userId;
        this.wordId = wordId;
        this.meaningId = meaningId;
        this.date = date;
    }

    public static StudyDto of (Long userId, Long wordId, Long meaningId, LocalDate date) {
        return StudyDto.builder()
                .userId(userId)
                .wordId(wordId)
                .meaningId(meaningId)
                .date(date)
                .build();
    }
}