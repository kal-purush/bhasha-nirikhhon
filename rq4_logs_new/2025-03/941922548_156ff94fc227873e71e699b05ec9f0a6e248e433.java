package com.gd.catalogapp.controller;

import com.gd.catalogapp.entity.Product;
import com.gd.catalogapp.service.CatalogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/catalog")
public class CatalogController {

    @Autowired
    private CatalogService catalogService;

    @GetMapping
    public Product getById(@RequestParam("uniqId") String uniqId) {

        return catalogService.getByUniqId(uniqId);
    }

    @GetMapping("/getbysku")
    public List<Product> getBySku(@RequestParam("sku") String sku) {

        return catalogService.getBySku(sku);
    }}
