package com.dasnnj.practice.weixinsell.service;

import com.dasnnj.practice.weixinsell.dataobject.ProductCategory;

import java.util.List;

/**
 * Description <P> DO : 商品类目service接口 <P>
 *
 * @author DASNNJ <a href="mailto:dasnnj@outlook.com"> dasnnj@outlook.com </a>
 * @date 2019 -05-15 21:18
 */
public interface IProductCategoryService {

    /**
     * Description <p> DO :  getOne</p>
     *
     * @param categoryId the category id
     * @return com.dasnnj.practice.weixinsell.dataobject.ProductCategory product category
     *
     * @author dasnnj
     * @date 2019 -05-15 09:21
     */
    ProductCategory getOne(Integer categoryId);

    /**
     * Description <p> DO : findAll </p>
     *
     * @return java.util.List<com.dasnnj.practice.weixinsell.dataobject.ProductCategory>    list
     *
     * @author dasnnj
     * @date 2019 -05-15 09:23
     */
    List<ProductCategory> findAll();

    /**
     * Description <p> DO :  根据类型categoryType查找</p>
     *
     * @param types the types
     * @return java.util.List<com.dasnnj.practice.weixinsell.dataobject.ProductCategory>   product categories by category type in
     *
     * @author dasnnj
     * @date 2019 -05-15 09:23
     */
    List<ProductCategory> getProductCategoriesByCategoryTypeIn(List<Integer> types);

    /**
     * Description <p> DO :  保存/更新</p>
     *
     * @param productCategory the product category
     * @return com.dasnnj.practice.weixinsell.dataobject.ProductCategory product category
     *
     * @author dasnnj
     * @date 2019 -05-15 09:24
     */
    ProductCategory save(ProductCategory productCategory);

}