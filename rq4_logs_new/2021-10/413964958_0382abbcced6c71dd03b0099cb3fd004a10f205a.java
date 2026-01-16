package edu.miu.cs590.SA.Ecommerce.stock;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
@RequestMapping("api")
public class StocksController {
    @PostMapping
    public String deduct(@RequestBody String val){
        log.info(val);
        return "OK";
    }
}