package com.example.catalogservice.controller;

import com.example.catalogservice.controller.dto.CatalogResponse.GetCatalogsResponse;
import com.example.catalogservice.mapper.CatalogMapper;
import com.example.catalogservice.service.CatalogService;
import com.example.catalogservice.service.dto.Catalog;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RequiredArgsConstructor
@RequestMapping(value = "/catalog-service")
@RestController
public class CatalogController {

    private final CatalogService catalogService;
    private final CatalogMapper catalogMapper;
    private final Environment env;

    @GetMapping("/health-check")
    public String status() {
        return String.format("It's Working in Catalog Service on Port %s",
            env.getProperty("local.server.port"));
    }

    @GetMapping("/catalogs")
    public ResponseEntity<GetCatalogsResponse> getCatalogs() {
        List<Catalog> catalogs = catalogService.getCatalogs();
        GetCatalogsResponse getCatalogsResponse = catalogMapper.toGetCatalogsResponse(catalogs);

        return ResponseEntity.status(HttpStatus.OK)
            .body(getCatalogsResponse);
    }
}