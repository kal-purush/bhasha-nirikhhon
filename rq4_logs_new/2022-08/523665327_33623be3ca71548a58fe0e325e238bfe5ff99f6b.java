package com.example.springbootcalculator;

import com.example.springbootcalculator.entity.Calculation;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface CalculatorRepository extends JpaRepository<Calculation, String> {

}