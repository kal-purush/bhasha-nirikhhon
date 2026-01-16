package com.rest.domain.Controller;

import com.rest.domain.Dto.RiversDto;
import com.rest.domain.Dto.SettlementsDto;
import com.rest.domain.Dto.assembler.RiversDtoAssembler;
import com.rest.domain.Dto.assembler.SettlementsDtoAssembler;
import com.rest.domain.Service.CountriesService;
import com.rest.domain.Service.RiversService;
import com.rest.domain.domain.Rivers;
import com.rest.domain.domain.Settlements;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.hateoas.CollectionModel;
import org.springframework.hateoas.Link;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.linkTo;
import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.methodOn;

@RestController
@RequestMapping(value = "/api/countries")
public class CountriesController {

    @Autowired
    private final CountriesService countriesService;

    public CountriesController(CountriesService countriesService) {
        this.countriesService = countriesService;
    }

    @PostMapping("/bunch")
    public ResponseEntity<Integer> createBunchOfCountries() {
        Integer bunch = countriesService.createBunchOfCountries();
        return new ResponseEntity<>(bunch, HttpStatus.OK);
    }

    @PostMapping("/withCursor")
    public ResponseEntity<String> createDatabasesUsingCursors() {
        countriesService.CreateDatabasesUsingCursors();
        return new ResponseEntity<>("New databases were created", HttpStatus.OK);
    }

}