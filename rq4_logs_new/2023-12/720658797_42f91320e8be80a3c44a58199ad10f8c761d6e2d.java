package com.jeontongju.product.client;

import com.jeontongju.product.dto.temp.FeignFormat;
import com.jeontongju.product.dto.temp.SellerInfoDto;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

@FeignClient(name = "sellerServiceClient", url = "${endpoint.seller-service}")
public interface SellerServiceClient {

  @GetMapping("/sellers/{sellerId}/name-image")
  FeignFormat<SellerInfoDto> getSellerInfoForCreateProduct(@PathVariable Long sellerId);
}