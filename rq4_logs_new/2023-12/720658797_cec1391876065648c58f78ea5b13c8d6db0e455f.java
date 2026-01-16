package com.jeontongju.product.mapper;

import com.jeontongju.product.domain.Category;
import com.jeontongju.product.domain.Product;
import com.jeontongju.product.domain.ProductDetailsImage;
import com.jeontongju.product.domain.ProductThumbnailImage;
import com.jeontongju.product.dto.request.ProductDto;
import com.jeontongju.product.dto.temp.SellerInfoDto;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ProductMapper {
  public Product toEntity(
      ProductDto productDto, Long sellerId, Category category, SellerInfoDto sellerInfoDto) {

    return Product.builder()
        .category(category)
        .sellerId(sellerId)
        .name(productDto.getProductName())
        .price(productDto.getProductPrice())
        .capacityToPriceRatio(
            Math.round(
                ((double) productDto.getProductPrice() / productDto.getProductCapacity()) * 100))
        .description(productDto.getProductDescription())
        .alcoholDegree(productDto.getProductAlcoholDegree())
        .capacity(productDto.getProductCapacity())
        .breweryName(productDto.getBreweryName())
        .breweryZoneCode(productDto.getBreweryZonecode())
        .breweryAddress(productDto.getBreweryAddress())
        .breweryAddressDetails(productDto.getBreweryAddressDetails())
        .manufacturer(productDto.getManufacturer())
        .stockQuantity(productDto.getRegisteredQuantity())
        .productThumbnailImage(
            ProductThumbnailImage.builder()
                .imageUrl(productDto.getProductThumbnailImageUrl())
                .build())
        .productDetailsImage(
            ProductDetailsImage.builder().imageUrl(productDto.getProductDetailsImageUrl()).build())
        .storeName(sellerInfoDto.getStoreName())
        .storeImageUrl(sellerInfoDto.getStoreImageUrl())
        .build();
  }
}