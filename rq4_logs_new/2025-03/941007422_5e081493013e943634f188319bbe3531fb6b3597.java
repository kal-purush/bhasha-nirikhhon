package com.example.auction.Global.config;

import com.example.auction.Auction.AuctionService;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class Scheduler {

    private final AuctionService auctionService;

    //  (cron = "0 0 00 * * *") 매일 00시
    //  (cron = "0/3 * * * * ?") 3초마다
    @Scheduled(cron = "0 0 00 * * *")
    public void statusUpdate(){
        auctionService.expiredAuction();
    }
}