package com.openpromt.coffeee.swf2023.openpromtserver.product.repository;

import com.openpromt.coffeee.swf2023.openpromtserver.product.entity.Product;
import com.openpromt.coffeee.swf2023.openpromtserver.product.util.ProductType;
import com.querydsl.jpa.impl.JPAQueryFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

import static com.openpromt.coffeee.swf2023.openpromtserver.product.entity.QProduct.product;


@Slf4j
@RequiredArgsConstructor
public class QueryDslProductRepositoryImpl implements QueryDslProductRepository{

    private final JPAQueryFactory jpaQueryFactory;

    @Override
    public List<Product> findListByProductType(ProductType product_type) {
        return jpaQueryFactory
                .selectFrom(product)
                .where(product.product_type.eq(product_type))
                .fetch();
    }

    @Override
    public List<Product> findListByCopyright_Id(Long copyright_id) {
        return jpaQueryFactory
                .selectFrom(product)
                .where(product.copyright.copyright_id.eq(copyright_id))
                .fetch();
    }
}