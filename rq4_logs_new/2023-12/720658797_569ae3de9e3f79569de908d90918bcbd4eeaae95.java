package com.jeontongju.product.controller;

import com.jeontongju.product.dto.temp.FeignFormat;
import com.jeontongju.product.dto.temp.ProductInfoDto;
import com.jeontongju.product.dto.temp.ProductSearchDto;
import com.jeontongju.product.service.ProductService;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
public class ProductClientController {

  private final ProductService productService;

  @PostMapping("/products")
  ResponseEntity<FeignFormat<List<ProductInfoDto>>> getProductInfoForOrder(
      @RequestBody ProductSearchDto productSearchDto) {

    return ResponseEntity.ok(
        FeignFormat.<List<ProductInfoDto>>builder()
            .code(HttpStatus.OK.value())
            .data(productService.getProductInfoForOrder(productSearchDto))
            .build());
  }
}