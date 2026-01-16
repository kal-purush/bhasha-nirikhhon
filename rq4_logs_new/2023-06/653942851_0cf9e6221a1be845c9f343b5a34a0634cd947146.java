package com.example.mealme.mapper;

import com.example.mealme.dto.ProductDto;
import com.example.mealme.vo.ProductVo;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface ProductMapper {


    //전체 조회(별점,총 개수,상품 이미지)
    List<ProductVo> selectAll();

    //음식 카테고리 조회
    List<ProductDto> foodCategory(String categoryName);

    //전체 음식 조회
    List<ProductVo> selectAllPrice();

    //낮은 가격 순으로 조회
    List<ProductVo> selectLowPrice();

    //높은 가격 순으로 조회
    List<ProductVo> selectReviewPrice();



}