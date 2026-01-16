package com.jinius.ecommerce.product.domain;

import com.jinius.ecommerce.Fixture;
import com.jinius.ecommerce.common.exception.LockException;
import com.jinius.ecommerce.product.domain.model.Stock;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

/**
 * 재고 처리 동시성 제어
 * (비동기 요청 처리 테스트)
 */
@SpringBootTest
@ActiveProfiles("test")
@AutoConfigureMockMvc
public class ConcurrencyTest {

    @Autowired
    StockService sut;

    @Autowired
    ProductRepository productRepository;

    @Test
    @DisplayName("동일한 상품을 3명의 유저가 동시에 1개씩 주문하는 경우, 실패 없이 모두 재고가 차감되어야 한다.")
    public void decreaseStock_concurrency() {
        //given
        Long productId = 1L;
        Long originQuantity = productRepository.findStockById(productId).get().getQuantity();

        //when
        CompletableFuture<Void> future1 = CompletableFuture.runAsync(() -> sut.decreaseStock(Fixture.orderItem()));
        CompletableFuture<Void> future2 = CompletableFuture.runAsync(() -> sut.decreaseStock(Fixture.orderItem()));
        CompletableFuture<Void> future3 = CompletableFuture.runAsync(() -> sut.decreaseStock(Fixture.orderItem()));
        List<CompletableFuture<Void>> futures = List.of(future1, future2, future3);

        futures.forEach(CompletableFuture::join);

        //then
        Stock result = productRepository.findStockById(productId).get();
        assert result.getProductId().equals(productId);
        assert result.getQuantity() == originQuantity - 3;
    }

    @Test
    @DisplayName("동일한 상품을 1000명의 유저가 동시에 1개씩 주문하는 경우, 실패 없이 모두 재고가 차감되어야 한다.")
    public void decreaseStock_concurrency_1000Request() {
        //given
        Long productId = 1L;
        Long originQuantity = productRepository.findStockById(productId).get().getQuantity();
        ExecutorService executor = Executors.newFixedThreadPool(20);

        //when
        try {
            List<CompletableFuture<Void>> futures = IntStream.range(0, 1000)
                    .mapToObj(i -> CompletableFuture.runAsync(() -> sut.decreaseStock(Fixture.orderItem()), executor))
                    .toList();

            futures.forEach(CompletableFuture::join);

            //then
            Stock result = productRepository.findStockById(productId).get();
            assert result.getProductId().equals(productId);
            assert result.getQuantity() == originQuantity - 1000;
        } finally {
            executor.shutdown();
        }
    }

    @Test
    @DisplayName("동일한 상품을 100명의 유저가 동시에 1개씩 주문하는 경우, 성공 카운트는 100개가 되어야 한다.")
    public void decreaseStock_concurrency_countingSuccess() {
        //given & when
        ExecutorService executor = Executors.newFixedThreadPool(20);

        List<CompletableFuture<Void>> futures = IntStream.range(0, 100)
                .mapToObj(i -> CompletableFuture.runAsync(() -> sut.decreaseStock(Fixture.orderItem()), executor))
                .toList();

        long successCount = futures.stream()
            .filter(future -> {
                try {
                    future.join();
                    return true;
                } catch (CompletionException e) {
                    if (e.getCause() instanceof LockException) {
                        return false;
                    }
                    throw e;
                } finally {
                    executor.shutdown();
                }
            })
            .count();

        //then
        System.out.println("successCount = " + successCount);
        assert successCount == 100;
    }
}