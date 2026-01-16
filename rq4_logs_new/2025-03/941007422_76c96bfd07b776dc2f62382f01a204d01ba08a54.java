package com.example.auction.AuctionRecord;

import com.example.auction.Auction.Auction;
import com.example.auction.Global.BaseEntity;
import com.example.auction.Point.PointReason;
import jakarta.persistence.*;
import lombok.Getter;

@Entity
@Getter
@Table(name = "auction_record")
public class AuctionRecord extends BaseEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private Long userId;

    @Column(nullable = false)
    private Long auctionId;

    @Column(nullable = false)
    private int bidPoint;

    @Enumerated(value = EnumType.STRING)
    @Column(nullable = false)
    private AuctionRecordStatus status = AuctionRecordStatus.BIDDING;

    public AuctionRecord(){}

    public AuctionRecord(Long userId , Long auctionId , int bidPoint){
        this.userId = userId;
        this.auctionId = auctionId;
        this.bidPoint = bidPoint;
    }

    public void auctionEnd(){
        this.status = AuctionRecordStatus.END;
    }

    public void setTopBid(Long userId , int bidPoint){
        this.userId = userId;
        this.bidPoint = bidPoint;
    }

}