package com.rljj.chipservice.domain.exchange.dto;

import com.rljj.chipservice.domain.chipinfo.dto.ChipInfoResponse;
import com.rljj.switchswitchentity.chip.chipexchange.ChipExchange;
import com.rljj.switchswitchentity.chip.chipexchange.ChipExchangeStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ChipExchangeResponse {
    private ChipInfoResponse chipInfo;
    private String nickname;
    private String content;
    private ChipExchangeStatus status;

    public static ChipExchangeResponse from(ChipExchange chipExchange) {
        return ChipExchangeResponse.builder()
                .chipInfo(ChipInfoResponse.from(chipExchange.getChipInfo()))
                .nickname(chipExchange.getMember().getNickname())
                .content(chipExchange.getContent())
                .status(chipExchange.getStatus())
                .build();
    }
}