package com.jeontongju.product.kafka;

import io.github.bitbox.bitbox.dto.OrderInfoDto;
import io.github.bitbox.bitbox.dto.SellerInfoDto;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ProductConsumer {

  @KafkaListener(topics = "delete-seller-to-product")
  public void deleteSellerToProduct(Long sellerId) {
    // 탈퇴 시키기
  }

  @KafkaListener(topics = "update-seller")
  public void updateSeller(SellerInfoDto sellerInfoDto) {
    // 수정 시키기
  }

  @KafkaListener(topics = "reduce-stock")
  public void reductStock (OrderInfoDto orderInfoDto)  {
    // 재고 확인 및 차감

  }

  @KafkaListener(topics = "add-stock")
  public void orderProduct ( )  {
    // 주문에서 터져서 롤백

  }

  @KafkaListener(topics = "update-product-sales-count")
  public void updateProductSalesCount( )  {
    // 주문 확정으로 상품 판매 개수 추가

  }

  @KafkaListener(topics = "cancel-order-stock")
  public void cancelOrderStock( )  {
    // 주문 취소 시 상품 재고 돌리기

  }

}