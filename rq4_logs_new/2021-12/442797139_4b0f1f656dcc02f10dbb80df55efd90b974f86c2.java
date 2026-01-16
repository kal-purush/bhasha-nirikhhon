package com.vinymt.course.repositories;

import org.springframework.data.jpa.repository.JpaRepository;

import com.vinymt.course.entities.Order;

public interface OrderRepository extends JpaRepository<Order, Long>{

}