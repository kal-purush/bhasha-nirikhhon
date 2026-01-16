package com.upgrad.FoodOrderingApp.api.controller;

import com.upgrad.FoodOrderingApp.api.model.ItemList;
import com.upgrad.FoodOrderingApp.api.model.ItemListResponse;
import com.upgrad.FoodOrderingApp.service.business.ItemService;
import com.upgrad.FoodOrderingApp.service.business.RestaurantService;
import com.upgrad.FoodOrderingApp.service.entity.ItemEntity;
import com.upgrad.FoodOrderingApp.service.entity.RestaurantEntity;
import com.upgrad.FoodOrderingApp.service.exception.RestaurantNotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

/**
 * Created by murarka on 07/06/21.
 */

@RestController
public class ItemController {
    @Autowired
    private RestaurantService restaurantService;

    @Autowired
    private ItemService itemService;

    @RequestMapping(method = RequestMethod.GET,
                    path = "/item/restaurant/{restaurantId}",
                    produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity<ItemListResponse> getItemList(@PathVariable("restaurant_id") final String restaurantUuid )
        throws RestaurantNotFoundException {

        RestaurantEntity restaurantEntity = restaurantService.restaurantByUUID(restaurantUuid);
        List<ItemEntity> itemEntityList = itemService.getTopFiveItems(restaurantEntity);

        ItemListResponse itemListResponse = new ItemListResponse();

        for(ItemEntity ie: itemEntityList) {
            ItemList itemList = new ItemList().id(UUID.fromString(ie.getUuid()))
                    .itemName(ie.getitemName())
                    .price(ie.getPrice());
            itemListResponse.add(itemList);


        }

        return new ResponseEntity<ItemListResponse>(itemListResponse, HttpStatus.OK);
    }
}