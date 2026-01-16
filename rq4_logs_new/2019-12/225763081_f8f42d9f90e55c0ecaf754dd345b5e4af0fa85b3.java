package com.lishuang.shopping.service;

import com.lishuang.shopping.entity.Goods;
import com.lishuang.shopping.mapper.GoodsRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class GoodsService {

    @Autowired
    GoodsRepository goodsRepository;

    public List<Goods> findGoodsByLikeName(String goodsName){

        List<Goods> goodsNameLike = goodsRepository.findGoodsByGoodsNameLike(goodsName);
        return goodsNameLike;
    }
}