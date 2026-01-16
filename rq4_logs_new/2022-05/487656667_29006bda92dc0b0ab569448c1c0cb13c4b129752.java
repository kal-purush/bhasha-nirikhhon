package com.group7.mezat.controllers;

import com.group7.mezat.documents.Auction;
import com.group7.mezat.documents.FishPackage;
import com.group7.mezat.requests.AuctionUpdateRequest;
import com.group7.mezat.services.AuctionService;
import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/auction")
@AllArgsConstructor
public class AuctionController {
    private AuctionService auctionService;

    @GetMapping
    public List<Auction> getAllAuction(){
        return auctionService.getAllAuction();
    }

    @GetMapping("/auctionId/{id}")
    public Auction getOneAuctionById(@PathVariable String id){
        return auctionService.getOneAuctionById(id);
    }


    @PostMapping
    public void addAuction(@RequestBody Auction auction){
        auctionService.addAuction(auction);
    }

    @DeleteMapping("/delete/{auctionId}")
    public void deleteAuction(@PathVariable String auctionId){
        auctionService.deleteAuction(auctionId);
    }

    @PutMapping("/update/{auctionId}")
    public void updateAuction(@PathVariable String auctionId, @RequestBody AuctionUpdateRequest updateRequest){
        auctionService.updateAuction(auctionId, updateRequest);
    }
    @PutMapping("/addFish/{auctionId}")
    public void addFishPackageToAuction(@RequestBody FishPackage fishPackage){
        auctionService.addFishPackageToAuction(fishPackage);
    }
}