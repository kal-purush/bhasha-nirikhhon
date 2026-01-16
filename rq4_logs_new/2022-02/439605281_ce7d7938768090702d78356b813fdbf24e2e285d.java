package com.example.demo.dao;

import org.springframework.data.jpa.repository.JpaRepository;

import com.example.demo.entities.PaymentInfo;

public interface PaymentInfoDAO extends JpaRepository<PaymentInfo, Integer>{

}