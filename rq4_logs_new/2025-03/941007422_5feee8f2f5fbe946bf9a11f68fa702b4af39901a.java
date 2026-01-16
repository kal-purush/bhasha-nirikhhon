package com.example.auction.Auction.Dto;

import com.example.auction.Auction.Auction;
import com.example.auction.Auction.AuctionStatus;
import lombok.Getter;

import java.time.LocalDateTime;

@Getter
public class AuctionResponseDto {

    private final Long id;

    private final Long userId;

    private final Long productId;

    private final int minPoint;

    private final AuctionStatus status;

    private final LocalDateTime expiredAt;

    private final LocalDateTime createdAt;

    private final LocalDateTime updatedAt;

    public AuctionResponseDto(Long id, Long userId, Long productId, int minPoint, AuctionStatus status, LocalDateTime expiredAt, LocalDateTime createdAt, LocalDateTime updatedAt) {
        this.id = id;
        this.userId = userId;
        this.productId = productId;
        this.minPoint = minPoint;
        this.status = status;
        this.expiredAt = expiredAt;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public static AuctionResponseDto toDto(Auction auction){
        return new AuctionResponseDto(
                auction.getId(),
                auction.getUserId(),
                auction.getProduct().getId(),
                auction.getMinPoint(),
                auction.getStatus(),
                auction.getExpiredAt(),
                auction.getCreatedAt(),
                auction.getUpdatedAt()
        );
    }
}