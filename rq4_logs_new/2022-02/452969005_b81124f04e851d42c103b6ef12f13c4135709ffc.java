package org.apache.ibatis;

import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.SystemMetaObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author liqiang
 * created on 2022-02-13 01:00
 */
public class UtilTest {

    @Test
    public void testMetaObject() {
        List<Order> orders = new ArrayList<>();
        orders.add(new Order("1234567", "早餐"));
        orders.add(new Order("4567899", "晚餐"));

        UserEntity userEntity = new UserEntity(orders, "Jack", 10);

        MetaObject metaObject = SystemMetaObject.forObject(userEntity);
        System.out.println(metaObject.getValue("orders[0].goodsName"));
        System.out.println(metaObject.getValue("orders[1].goodsName"));

        metaObject.setValue("orders[0].orderNo", "order_12345");

        System.out.println("has orderNo: " + metaObject.hasGetter("orderNo"));
        System.out.println("has name: " + metaObject.hasGetter("name"));
    }

    private static class UserEntity {

        public UserEntity(List<Order> orders, String name, Integer age) {
            this.orders = orders;
            this.name = name;
            this.age = age;
        }

        List<UtilTest.Order> orders;
        String name;
        Integer age;

    }

    private static class Order {

        public Order(String orderNo, String goodsName) {
            this.orderNo = orderNo;
            this.goodsName = goodsName;
        }

        String orderNo;
        String goodsName;
    }

}