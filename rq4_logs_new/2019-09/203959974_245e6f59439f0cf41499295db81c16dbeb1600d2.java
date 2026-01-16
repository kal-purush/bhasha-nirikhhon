package com.study.sell.mysell.dao;

import com.study.sell.mysell.entity.OrderInfo;
import org.springframework.data.jpa.repository.JpaRepository;


public interface OrderInfoDao extends JpaRepository<OrderInfo, String> {

}