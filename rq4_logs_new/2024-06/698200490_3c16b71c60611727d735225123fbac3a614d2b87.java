package org.petmarket.advertisements.advertisement.listener;

import jakarta.persistence.PostPersist;
import jakarta.persistence.PostUpdate;
import org.petmarket.advertisements.advertisement.entity.Advertisement;
import org.petmarket.advertisements.advertisement.service.AdvertisementService;
import org.springframework.context.annotation.Lazy;

public class AdvertisementListener {
//    private final AdvertisementService advertisementService;
//
//    public AdvertisementListener(@Lazy AdvertisementService advertisementService) {
//        this.advertisementService = advertisementService;
//    }
//
//    @PostPersist
//    @PostUpdate
//    public void updateTopRating(Advertisement advertisement) {
//        advertisementService.updateTopRating(advertisement);
//    }
}