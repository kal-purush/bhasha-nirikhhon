package com.group7.mezat.responses;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.group7.mezat.documents.Auction;
import com.group7.mezat.documents.AuctionStatus;
import com.group7.mezat.documents.Bid;
import com.group7.mezat.documents.FishPackage;
import lombok.Data;

import java.util.Date;
import java.util.List;

@Data
public class AuctionResponse {
    private String id;
    private AuctionStatus auctionStatus;
    @JsonFormat(pattern="yyyy-MM-dd'T'HH:mm:ss", timezone = "GMT+03:00")
    private Date auctionStart;
    @JsonFormat(pattern="yyyy-MM-dd'T'HH:mm:ss", timezone = "GMT+03:00")
    private Date auctionEnd;
    private List<FishPackage> fishList;
    private List<Bid> bidList;


    public AuctionResponse(Auction auction) {
        this.id = id;
        this.auctionStatus = auction.getAuctionStatus();
        this.auctionStart = auction.getAuctionStart();
        this.auctionEnd = auction.getAuctionEnd();
        this.fishList = auction.getFishList();
        this.bidList = auction.getBidList();
    }
}