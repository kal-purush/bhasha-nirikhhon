package com.challenger.fridge.service;

import com.challenger.fridge.domain.CartItem;
import com.challenger.fridge.domain.StorageItem;
import com.challenger.fridge.domain.box.StorageBox;
import com.challenger.fridge.dto.cart.CartItemToStorageRequest;
import com.challenger.fridge.dto.cart.CartItemToStorageRequest.CartItemRequest;
import com.challenger.fridge.repository.CartItemRepository;
import com.challenger.fridge.repository.StorageBoxRepository;
import com.challenger.fridge.repository.StorageItemRepository;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class CartStorageService {

    private final CartItemRepository cartItemRepository;
    private final StorageBoxRepository storageBoxRepository;
    private final StorageItemRepository storageItemRepository;

    @Transactional
    public void moveItems(CartItemToStorageRequest request) {
        StorageBox storageBox = storageBoxRepository.findById(request.getBoxId())
                .orElseThrow(() -> new IllegalArgumentException("세부 보관소를 찾을 수 없습니다."));

        // cartItem 찾고 storageItem 으로 변환해서 저장
        List<StorageItem> storageItemList = request.getCartItemRequests().stream()
                .map(r -> {
                    CartItem cartItem = cartItemRepository.findItemsById(r.getCartItemId());
                    return new StorageItem(r.getCount(), cartItem.getItem(), storageBox);
                }).toList();

        // bulk insert
        storageItemRepository.saveAll(storageItemList);

        // cartItem 삭제
        List<Long> cartItemIdList = request.getCartItemRequests().stream()
                .map(CartItemRequest::getCartItemId)
                .toList();
        cartItemRepository.deleteAllByIdInBatch(cartItemIdList);
    }
}