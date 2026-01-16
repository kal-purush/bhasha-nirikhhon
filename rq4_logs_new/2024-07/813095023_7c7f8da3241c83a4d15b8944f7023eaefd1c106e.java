package com.spring.mood.projectmvc.service;

import com.spring.mood.projectmvc.mapper.CartArchiveMapper;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class CartArchiveService {
    private final CartArchiveMapper cartArchiveMapper;
    private static final Logger log = LoggerFactory.getLogger(CartArchiveService.class);

    @Transactional
    public void copyCartToArchive(String userAccount) {
        log.info("Copying cart items to archive for userAccount: {}", userAccount);
        cartArchiveMapper.copyToCartArchive(userAccount);
    }

    public List<Long> getCartArchiveIds(String userAccount) {
        log.info("Getting cart archive IDs for userAccount: {}", userAccount);
        return cartArchiveMapper.getCartArchiveIds(userAccount);
    }

    public List<Map<String, Object>> getOrderHistory(String userAccount) {
        return cartArchiveMapper.getOrderHistory(userAccount);
    }
}