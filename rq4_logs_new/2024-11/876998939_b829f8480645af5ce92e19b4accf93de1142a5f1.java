package com.smartverse.churchlitebackend.config.security.handler;


import com.potatotech.authorization.stereotype.Anonymous;
import com.smartverse.churchlitebackend.config.security.model.UserSupplierDTO;
import com.smartverse.churchlitebackend.config.security.service.AuthenticationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Hashtable;

@RestController
@CrossOrigin(origins="*")
@RequestMapping
public class AuthenticationHandlerImpl {

    @Autowired
    AuthenticationService authenticationService;

    @Anonymous
    @PostMapping("/authenticate")
    public ResponseEntity<?> authenticate(@RequestBody UserSupplierDTO userSupplier){
        var map = new Hashtable<>();
        map.put("accessToken", authenticationService.login(userSupplier));
        map.put("validate", 80000);
        return ResponseEntity.ok(map);
    }

    @Anonymous
    @PostMapping("/register")
    public ResponseEntity<?> register(@RequestBody UserSupplierDTO userSupplier){
        var map = new Hashtable<>();
        map.put("accessToken", authenticationService.login(userSupplier));
        map.put("validate", 80000);
        return ResponseEntity.ok(map);
    }
}