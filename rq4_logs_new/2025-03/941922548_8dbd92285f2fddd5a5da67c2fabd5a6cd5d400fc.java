package com.gd.inventoryapp.controller;

import com.gd.inventoryapp.entity.Inventory;
import com.gd.inventoryapp.service.InventoryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class InventoryController {

    @Autowired
    InventoryService inventoryService;

    @GetMapping("/getinv")
    List<Inventory> getAvailability() {
        inventoryService.fillRandomAvailability();
        return inventoryService.getByUniqId();
    }

    @GetMapping("/getinvbyid")
    List<Inventory> getAvailability(@RequestParam("uniqIdList") List <String> uniqIdList) {
     //   inventoryService.fillRandomAvailability();

        return inventoryService.getListInventory(uniqIdList);
    }

}
