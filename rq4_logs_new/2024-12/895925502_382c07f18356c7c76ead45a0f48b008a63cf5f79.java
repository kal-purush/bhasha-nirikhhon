package com.mulmeong.notification.dto;

import com.mulmeong.notification.vo.ReadInfoVo;
import lombok.Builder;
import lombok.Getter;
import lombok.Locked;

@Getter
@Builder
public class ReadInfoDto {
    private boolean read;
    private Integer count;

    public static ReadInfoDto toDto(boolean read, Integer count) {
        return ReadInfoDto.builder()
                .read(read)
                .count(count)
                .build();
    }

    public ReadInfoVo toVo() {
        return ReadInfoVo.builder()
                .read(read)
                .count(count)
                .build();
    }
}