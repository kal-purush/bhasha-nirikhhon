package com.openlabs.shoppingmall.controller;

import com.openlabs.framework.dto.ResponseDto;
import com.openlabs.shoppingmall.dto.ItemDto;
import com.openlabs.shoppingmall.entity.Items;
import com.openlabs.shoppingmall.service.ItemService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Api(tags = {"상품 관련 API LIST"})
@RestController
@RequestMapping("/labshop/v1/user")
@RequiredArgsConstructor
@Slf4j
public class ItemController {
    private final ItemService itemService;

//    @GetMapping("r-item")
//    @ApiOperation(value = "상품 검색")
//    public ResponseDto<Slice<Items>> searchItem(@RequestParam String itemName, Pageable pageable) {
//        return ResponseDto.ok(itemService.searchItem(itemName, pageable));
//    }
}