package com.geekbrains;

import com.geekbrains.db.dao.ProductsMapper;
import com.geekbrains.db.model.Products;
import com.geekbrains.db.model.ProductsExample;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.IOException;
import java.util.List;

@Slf4j
public class Main {

    public static void main(String[] args) throws InterruptedException, IOException {

        SqlSessionFactory sessionFactory = new SqlSessionFactoryBuilder()
                .build(Resources.getResourceAsStream("myBatisConfig.xml"));

        SqlSession session = sessionFactory.openSession();

        ProductsMapper productsMapper = session.getMapper(ProductsMapper.class);

        Products product = productsMapper.selectByPrimaryKey(3L);
        log.info("product: {}", product);

        ProductsExample filter = new ProductsExample();

        filter.createCriteria()
                .andTitleGreaterThan("a")
                .andTitleLessThan("h");

        List<Products> products = productsMapper.selectByExample(filter);
        products.forEach(p -> log.info("product: {}", p));

        Products newProduct = new Products();
        newProduct.setPrice(12);
        newProduct.setTitle("New product");
        newProduct.setCategoryId(1L);

        productsMapper.insertSelective(newProduct);

        filter.clear();
        filter.createCriteria()
                .andTitleEqualTo("New product");

        List<Products> newProducts = productsMapper.selectByExample(filter);

        newProducts.forEach(p -> log.info("product new: {}", p));

        session.commit();

        log.info("Hello world! 2");
    }
}