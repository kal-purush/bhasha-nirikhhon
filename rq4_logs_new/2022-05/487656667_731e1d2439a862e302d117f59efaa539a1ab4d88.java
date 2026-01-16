package com.group7.mezat.requests;

import lombok.Data;

@Data
public class BidRequest {

    private String bidderId;
    private String auctionId;
    private float bid;
    private String fishPackageId;
}