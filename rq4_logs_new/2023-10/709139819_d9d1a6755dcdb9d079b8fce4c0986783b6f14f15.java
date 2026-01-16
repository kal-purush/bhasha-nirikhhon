package com.thanksd.server.dto.response;

import lombok.*;

import java.time.LocalDate;
import java.util.List;

@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor
@ToString
public class DiaryDateResponse {
    private List<LocalDate> dateList;
}