package com.challenger.fridge.service;

import static org.assertj.core.api.Assertions.*;

import com.challenger.fridge.domain.Item;
import com.challenger.fridge.domain.Storage;
import com.challenger.fridge.domain.StorageItem;
import com.challenger.fridge.domain.box.StorageBox;
import com.challenger.fridge.dto.cart.CartItemRequest;
import com.challenger.fridge.dto.cart.CartItemMoveRequest;
import com.challenger.fridge.dto.cart.CartResponse;
import com.challenger.fridge.dto.sign.SignUpRequest;
import com.challenger.fridge.dto.storage.request.StorageSaveRequest;
import com.challenger.fridge.repository.StorageBoxRepository;
import com.challenger.fridge.repository.StorageRepository;
import jakarta.persistence.EntityManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

@SpringBootTest
@Transactional
class CartStorageServiceTest {

    @Autowired SignService signService;
    @Autowired CartService cartService;
    @Autowired StorageService storageService;
    @Autowired EntityManager em;
    @Autowired StorageRepository storageRepository;
    @Autowired StorageBoxRepository storageBoxRepository;
    @Autowired CartStorageService cartStorageService;

    Long storageId;
    List<Item> itemList = new ArrayList<>();
    List<Long> cartItemIdList;

    @BeforeEach
    void setUp() {
        String email = "jjw@test.com";
        signService.registerMember(new SignUpRequest("jjw@test.com", "1234", "jjj"));
        storageId = storageService.saveStorage(new StorageSaveRequest("퍼스트 보관소", 3L, 2L), email);
        Item item1 = em.createQuery("select i from Item i where i.itemName  = '돼지고기'", Item.class).getSingleResult();
        Item item2 = em.createQuery("select i from Item i where i.itemName  = '양파'", Item.class).getSingleResult();
        Item item3 = em.createQuery("select i from Item i where i.itemName  = '대파'", Item.class).getSingleResult();
        Item item4 = em.createQuery("select i from Item i where i.itemName  = '마늘'", Item.class).getSingleResult();
        itemList.addAll(Arrays.asList(item1, item2, item3, item4));

        cartItemIdList = itemList.stream()
                .map(item -> cartService.addItem(email, item.getId())).toList();
    }

    @DisplayName("장바구니에서 모든 상품을 보관소로 옮기기")
    @Test
    void moveItemsToBox() {
        Storage storage = storageRepository.findById(storageId)
                .orElseThrow(IllegalArgumentException::new);
        Long boxId = storage.getStorageBoxList().get(1).getId();
        CartItemRequest pork = new CartItemRequest(cartItemIdList.get(0), 1L);
        CartItemRequest onion = new CartItemRequest(cartItemIdList.get(1), 2L);
        CartItemRequest greenOnion = new CartItemRequest(cartItemIdList.get(2), 3L);
        CartItemRequest garlic = new CartItemRequest(cartItemIdList.get(3), 4L);
        List<CartItemRequest> cartItemRequests = new ArrayList<>();
        cartItemRequests.add(pork);
        cartItemRequests.add(onion);
        cartItemRequests.add(greenOnion);
        cartItemRequests.add(garlic);
        CartItemMoveRequest cartItemMoveRequest = new CartItemMoveRequest(boxId, cartItemRequests);

        cartStorageService.moveItems(cartItemMoveRequest);
        CartResponse res = cartService.findItems("jjw@test.com");
        StorageBox storageBox = storageBoxRepository.findStorageItemsById(boxId)
                .orElseThrow(IllegalArgumentException::new);
        List<StorageItem> storageItemList = storageBox.getStorageItemList();

        assertThat(res.getCartItems().size()).isEqualTo(0);

        assertThat(storageItemList.get(0).getItem().getItemName()).isEqualTo("돼지고기");
        assertThat(storageItemList.get(1).getItem().getItemName()).isEqualTo("양파");
        assertThat(storageItemList.get(2).getItem().getItemName()).isEqualTo("대파");
        assertThat(storageItemList.get(3).getItem().getItemName()).isEqualTo("마늘");

        assertThat(storageItemList.get(0).getQuantity()).isEqualTo(1);
        assertThat(storageItemList.get(1).getQuantity()).isEqualTo(2);
        assertThat(storageItemList.get(2).getQuantity()).isEqualTo(3);
        assertThat(storageItemList.get(3).getQuantity()).isEqualTo(4);
    }

    @DisplayName("장바구니에서 선택한 상품을 보관소로 옮기기")
    @Test
    void moveSelectedItemsToBox() {
        Storage storage = storageRepository.findById(storageId)
                .orElseThrow(IllegalArgumentException::new);
        Long boxId = storage.getStorageBoxList().get(1).getId();
        CartItemRequest onion = new CartItemRequest(cartItemIdList.get(1), 2L);
        CartItemRequest greenOnion = new CartItemRequest(cartItemIdList.get(2), 3L);
        List<CartItemRequest> cartItemRequests = new ArrayList<>();
        cartItemRequests.add(onion);
        cartItemRequests.add(greenOnion);
        CartItemMoveRequest cartItemMoveRequest = new CartItemMoveRequest(boxId, cartItemRequests);

        cartStorageService.moveItems(cartItemMoveRequest);

        CartResponse res = cartService.findItems("jjw@test.com");
        StorageBox storageBox = storageBoxRepository.findStorageItemsById(boxId)
                .orElseThrow(IllegalArgumentException::new);
        List<StorageItem> storageItemList = storageBox.getStorageItemList();

        assertThat(res.getCartItems().size()).isEqualTo(2);

        assertThat(storageItemList.get(0).getItem().getItemName()).isEqualTo("양파");
        assertThat(storageItemList.get(1).getItem().getItemName()).isEqualTo("대파");

        assertThat(storageItemList.get(0).getQuantity()).isEqualTo(2);
        assertThat(storageItemList.get(1).getQuantity()).isEqualTo(3);
    }

}