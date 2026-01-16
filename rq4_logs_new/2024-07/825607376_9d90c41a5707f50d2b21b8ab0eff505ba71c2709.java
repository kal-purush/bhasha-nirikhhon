package refooding.api.domain.exchange.dto.response;

import com.querydsl.core.annotations.QueryProjection;
import refooding.api.domain.exchange.entity.ExchangeStatus;

import java.time.LocalDateTime;

public record ExchangeResponse(
        Long exchangeId,
        String title,
        String region,
        String childRegion,
        ExchangeStatus status,
        LocalDateTime createDate
        // TODO : 이미지
        // TODO : 회원
) {
    @QueryProjection
    public ExchangeResponse {
    }
}