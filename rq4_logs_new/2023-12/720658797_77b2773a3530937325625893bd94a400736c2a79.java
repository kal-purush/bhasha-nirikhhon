package com.jeontongju.product.service;

import com.jeontongju.product.domain.Category;
import com.jeontongju.product.domain.Product;
import com.jeontongju.product.domain.ProductDetailsImage;
import com.jeontongju.product.domain.ProductThumbnailImage;
import com.jeontongju.product.repository.CategoryRepository;
import com.jeontongju.product.repository.ProductDetailsImageRepository;
import com.jeontongju.product.repository.ProductRepository;
import com.jeontongju.product.repository.ProductThumbnailImageRepository;
import io.github.bitbox.bitbox.dto.ProductUpdateDto;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

@SpringBootTest
@Transactional
public class ProductServiceTest {

  @Autowired private ProductService productService;
  @Autowired private CategoryRepository categoryRepository;
  @Autowired private ProductRepository productRepository;
  @Autowired private ProductThumbnailImageRepository productThumbnailImageRepository;
  @Autowired private ProductDetailsImageRepository productDetailsImageRepository;

  private Category initCategory() {
    Category category = Category.builder().name("막걸리").build();
    return categoryRepository.save(category);
  }

  private Product initProduct() {
    Category category = initCategory();
    ProductThumbnailImage productThumbnailImageEntity =
        ProductThumbnailImage.builder().imageUrl("/thumbnail/example").build();
    ProductDetailsImage productDetailsImageEntity =
        ProductDetailsImage.builder().imageUrl("/details/example").build();

    Long price = 10000L;
    Long capacity = 936L;

    Product productEntity =
        Product.builder()
            .name("복순도가")
            .category(category)
            .sellerId(1L)
            .price(price)
            .capacity(capacity)
            .capacityToPriceRatio(Math.round(((double) price / capacity) * 100))
            .description("복순복순")
            .alcoholDegree(17.1)
            .breweryName("복순 양조장")
            .breweryAddress("서울특별시 강동구")
            .breweryZoneCode("01111")
            .breweryAddressDetails("105동")
            .manufacturer("우리도가")
            .stockQuantity(2L)
            .productThumbnailImage(productThumbnailImageEntity)
            .productDetailsImage(productDetailsImageEntity)
            .storeName("우리우리도가")
            .storeImageUrl("/store/example")
            .build();

    return productRepository.save(productEntity);
  }

  @Test
  @DisplayName("주문 시, 재고 감소 - 비관적락")
  public void reduceStock() throws InterruptedException {
    int threadCount = 100;
    ExecutorService executorService = Executors.newFixedThreadPool(32);
    CountDownLatch latch = new CountDownLatch(threadCount);

    List<ProductUpdateDto> productUpdateDtoList = new ArrayList<>();

    Product product = initProduct();
    productUpdateDtoList.add(
        ProductUpdateDto.builder().productId(product.getProductId()).productCount(1L).build());

    for (int i = 0; i < threadCount; i++) {
      executorService.submit(
          () -> {
            try {
              productService.reduceStock(productUpdateDtoList);
            } finally {
              latch.countDown();
            }
          });
    }

    latch.await();

    Product findProduct = productRepository.findById(product.getProductId()).orElseThrow();

    Assertions.assertEquals(0L, findProduct.getStockQuantity()); // success
  }
}