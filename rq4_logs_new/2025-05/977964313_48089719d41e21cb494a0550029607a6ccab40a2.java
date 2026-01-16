package org.skypro.skyshop;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.skypro.skyshop.model.basket.BasketItem;
import org.skypro.skyshop.model.basket.ProductBasket;
import org.skypro.skyshop.model.basket.UserBasket;
import org.skypro.skyshop.model.exception.NoSuchProductException;
import org.skypro.skyshop.model.product.Product;
import org.skypro.skyshop.model.service.BasketService;
import org.skypro.skyshop.model.service.StorageService;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class BasketServiceTest {
    private Product existingProduct;
    private UUID existingProductId;
    private UUID nonExistingProductId;

    @Mock
    private ProductBasket productBasket;

    @Mock
    private StorageService storageService;

    @InjectMocks
    private BasketService basketService;

    @BeforeEach
    void setUp() {
        existingProduct = mock(Product.class);
        existingProductId = UUID.randomUUID();
        nonExistingProductId = UUID.randomUUID();
    }

    //Добавление несуществующего товара в корзину приводит к выбросу исключения
    @Test
    void addProduct_NonExistingProduct_ThrowsNoSuchProductException() {
        when(storageService.getProductById(nonExistingProductId)).thenReturn(Optional.empty());

        assertThrows(NoSuchProductException.class, () -> basketService.addProduct(nonExistingProductId));

        verify(productBasket, never()).addProduct(any());
    }

    //Добавление существующего товара вызывает метод addProduct у мока ProductBasket
    @Test
    void addProduct_ExistingProduct_CallsAddProductOnProductBasket() {
        when(storageService.getProductById(existingProductId)).thenReturn(Optional.of(existingProduct));

        basketService.addProduct(existingProductId);

        verify(productBasket, times(1)).addProduct(existingProductId);
    }

    //Метод getUserBacket возвращает пустую корзину, если ProductBasket пуст
    @Test
    void getUserBasket_EmptyBasket_ReturnsEmptyUserBasket() {
        when(productBasket.getItems()).thenReturn(Collections.emptyMap());

        UserBasket userBasket = basketService.getUserBasket();

        assertNotNull(userBasket);
        assertTrue(userBasket.getItems().isEmpty());
        assertEquals(0, userBasket.getTotal());
    }
    // Метод getUserBasket возвращает подходящую корзину, если в ProductBasket есть товары.
    @Test
    void getUserBasket_NonEmptyBasket_ReturnsCorrectUserBasket() {
        // Мокируем корзину с одним товаром
        Map<UUID, Integer> itemsMap = Collections.singletonMap(existingProductId, 3);
        when(productBasket.getItems()).thenReturn(itemsMap);
        when(storageService.getProductById(existingProductId)).thenReturn(Optional.of(existingProduct));
        when(existingProduct.getPrice()).thenReturn(100);

        UserBasket userBasket = basketService.getUserBasket();

        assertNotNull(userBasket);

        assertEquals(1, userBasket.getItems().size());

        assertEquals(300, userBasket.getTotal());

        BasketItem basketItem = userBasket.getItems().get(0);
        assertEquals(existingProduct, basketItem.getProduct());
        assertEquals(3, basketItem.getQuantity());
    }
}
