package com.boot.DTO;

import java.time.LocalDateTime;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PthistDTO {
    private String uuid;  // 사용자 UUID
    private String pthistno;  // 이력번호 pt240909-0001
    private String kind;  // 유형(적립,사용,소멸) 
    private String description;  // 상세(관란권구매,유효기간만료,출석이벤트) 
    private int recqt;  // 획득 포인트 (적립 포인트)
    private int issqt;  // 사용 포인트 (차감 포인트)
    private LocalDateTime trndt; //이력일자 (default: now()) 
}