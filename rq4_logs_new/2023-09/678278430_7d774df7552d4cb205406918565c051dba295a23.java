package com.openlabs.shoppingmall.controller;

import com.openlabs.framework.dto.ResponseDto;
import com.openlabs.framework.exception.ShopException;
import com.openlabs.shoppingmall.dto.ItemsReqDto;
import com.openlabs.shoppingmall.dto.ItemsResDto;
import com.openlabs.shoppingmall.service.ItemsService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Slice;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/labshop/v1/admin")
@Api(value = "상품관련 API")
public class ItemsController {
    @Autowired
    ItemsService service;

    /** 상품등록
     *
     * @param reqDto
     * @return  ItemsResDto
     * */
    @PostMapping("/c-items")
    @ApiOperation(value = "상품등록")
    public ResponseDto<ItemsResDto> createItems(ItemsReqDto reqDto) {
        try {
            return ResponseDto.ok(service.createItems(reqDto));
        } catch (ShopException e) {
            return ResponseDto.error(401, ",상품등록에 실패했습니다.", null, null);
        }
    }
    /** 상품수정
     *
     * @param reqDto
     * @return  ItemsResDto
     * */
    @PutMapping("/u-items")
    @ApiOperation(value = "상품수정")
    public ResponseDto<ItemsResDto> updateItems(ItemsReqDto reqDto) {
        try {
            return ResponseDto.ok(service.updateItems(reqDto));
        } catch (ShopException e) {
            return ResponseDto.error(401, "상품수정에 실패했습니다.", null, null);
        }
    }
    /** 상품삭제
     *
     * @param itemId
     * @return  itemId
     * */
    @DeleteMapping("/d-items")
    @ApiOperation(value = "쿠폰삭제")
    public ResponseDto<Long> deleteItems(Long itemId) {
        try {
            service.deleteItems(itemId);
            return ResponseDto.ok(itemId);
        } catch (ShopException e) {
            return ResponseDto.error(401, "상품삭제에 실패했습니다.", null, null);
        }
    }
    /** 상품목록조회
     *
     * @param reqDto
     * @return  UserResDto
     * */
    @GetMapping("/r-items")
    @ApiOperation(value = "상품목록조회")
    public ResponseDto<Slice<ItemsResDto>> multiQueryItems(ItemsReqDto reqDto
            , @RequestParam(name = "page", defaultValue = "0") int page
            , @RequestParam(name = "size", defaultValue = "10") int size
//            , @RequestParam PageDto pageDto
    ) {
        try {
            return ResponseDto.ok(service.multiQueryItems(reqDto, page, size));
//            return ResponseDto.ok(service.multiQueryItems(reqDto, pageDto));
        } catch (ShopException e) {
            return ResponseDto.error(401, "쿠폰등록에 실패했습니다.", null, null);
        }
    }
    /** 상품상세조회
     *
     * @param itemId
     * @return  ItemsResDto
     * */
    @GetMapping("/r-items/detail")
    @ApiOperation(value = "상품상세조회")
    public ResponseDto<ItemsResDto> singleQueryItem(Long itemId) {
        try {
            return ResponseDto.ok(service.singleQueryItem(itemId));
        } catch (ShopException e) {
            return ResponseDto.error(401, "실패", null, null);
        }
    }
}