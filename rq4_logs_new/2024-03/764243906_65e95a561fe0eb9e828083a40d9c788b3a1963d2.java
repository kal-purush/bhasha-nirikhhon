package dev.enricosola.porcellino.controller;

import dev.enricosola.porcellino.response.currency.ListResponse;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import dev.enricosola.porcellino.service.CurrencyService;
import org.springframework.http.ResponseEntity;

@RestController
@RequestMapping("/api/currency")
@CrossOrigin(origins = "*", maxAge = 3600)
public class CurrencyController {
    private final CurrencyService currencyService;

    public CurrencyController(CurrencyService currencyService){
        this.currencyService = currencyService;
    }

    @GetMapping()
    public ResponseEntity<ListResponse> list(){
        return ResponseEntity.ok().body(new ListResponse(this.currencyService.getAll()));
    }
}