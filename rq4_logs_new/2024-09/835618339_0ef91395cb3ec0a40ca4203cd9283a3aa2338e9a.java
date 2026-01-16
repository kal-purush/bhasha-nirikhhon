package com.kh.erp.restcontroller;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kh.erp.dao.ErdDao;
import com.kh.erp.dto.ErdDto;

@RestController
@RequestMapping("/api")
public class CategoryDataController {

    private final ErdDao erdDao;

    public CategoryDataController(ErdDao erdDao) {
        this.erdDao = erdDao;
    }

    @GetMapping("/categoryQuantityData")
    public Map<String, Integer> getCategoryQuantityData() {
        try {
            // 모든 상품 조회
            List<ErdDto> allProducts = erdDao.selectList(); // 기존 메서드 사용

            // 카테고리별 수량 집계
            Map<String, Integer> categoryMap = new HashMap<>();
            for (ErdDto product : allProducts) {
                String category = product.getStockCategory(); // 카테고리
                int quantity = product.getStockQuantity(); // 수량
                categoryMap.put(category, categoryMap.getOrDefault(category, 0) + quantity);
            }

            // Map을 JSON으로 변환하여 반환
            return categoryMap;
        } catch (Exception e) {
            e.printStackTrace(); // 로그에 에러 기록
            return new HashMap<>(); // 빈 맵 반환
        }
    }
}