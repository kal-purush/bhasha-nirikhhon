package com.example.auction.Point.Dto;

import com.example.auction.Point.Point;
import com.example.auction.Point.PointReason;
import lombok.Getter;

import java.time.LocalDateTime;

@Getter
public class PointEarnResponseDto {

    private final Long id;

    private final Long userId;

    private final PointReason reason;

    private final int usePoint;

    private final int totalPoint;

    private final LocalDateTime createdAt;

    private final LocalDateTime updatedAt;

    public PointEarnResponseDto(Long id, Long userId, PointReason reason, int usePoint, int totalPoint, LocalDateTime createdAt, LocalDateTime updatedAt) {
        this.id = id;
        this.userId = userId;
        this.reason = reason;
        this.usePoint = usePoint;
        this.totalPoint = totalPoint;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public static PointEarnResponseDto toDto(Point point){
        return new PointEarnResponseDto(
                point.getId(),
                point.getUserId(),
                point.getReason(),
                point.getUsePoint(),
                point.getTotalPoint(),
                point.getCreatedAt(),
                point.getUpdatedAt()
        );
    }
}