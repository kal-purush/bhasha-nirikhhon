package com.inventory.inventoryService.api;


import com.inventory.inventoryService.utils.ResponseHandler;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
public class InventoryAPI {

    @GetMapping("/hello")
    public ResponseEntity<Object> hello() {
        Map<String, String> helloWorld = new HashMap<String, String>();
        helloWorld.put("message", "Hello World");

        return ResponseHandler.generateResponse(helloWorld);
    }
}