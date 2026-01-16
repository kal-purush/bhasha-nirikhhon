package refooding.api.domain.exchange.dto.response;

import refooding.api.domain.exchange.entity.Exchange;
import refooding.api.domain.exchange.entity.ExchangeStatus;

import java.time.LocalDateTime;

public record ExchangeDetailResponse(
        Long id,
        String title,
        String content,
        String region,
        String childRegion,
        ExchangeStatus status,
        LocalDateTime createDate
        // 회원 추가
        // 이미지 추가
) {
    public static ExchangeDetailResponse from(Exchange exchange) {
        return new ExchangeDetailResponse(
                exchange.getId(),
                exchange.getTitle(),
                exchange.getContent(),
                exchange.getRegion().getParent().getName(),
                exchange.getRegion().getName(),
                exchange.getStatus(),
                exchange.getCreatedDate());
    }
}