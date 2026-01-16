package com.eidiko.supermarket_action_service.dao;

import com.eidiko.supermarket_action_service.dto.SalesDto;
import com.eidiko.supermarket_action_service.model.Sales;
import com.eidiko.supermarket_action_service.model.Stocks;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

@Repository
public class SalesRepo {


    private final JdbcTemplate jdbcTemplate;
    private final JdbcTemplate postJdbcTemplate;

    @Autowired
    public SalesRepo(@Qualifier("mssqlJdbcTemplate") JdbcTemplate jdbcTemplate,@Qualifier("postgresqlJdbcTemplate")JdbcTemplate postgresJdbc)
    {
        this.jdbcTemplate=jdbcTemplate;
        this.postJdbcTemplate=postgresJdbc;
    }

    public Sales addSales(SalesDto salesDto)
    {
        List<Stocks>list =salesDto.getStocks();
        List<Integer>resultList=new ArrayList<>();
        double totalAmount=0;
        for(Stocks stocks:list)
        {
            String stockQuery="select * from stocks where id= ?";
            Stocks stocks1=jdbcTemplate.queryForObject(stockQuery,new BeanPropertyRowMapper<>(Stocks.class),stocks.getId());
            assert stocks1 != null;
            resultList.add(stocks1.getId());
            double total=stocks1.getPrice()*stocks.getQuantity();
            totalAmount=totalAmount+total;
        }
        salesDto.setSalesAmount(totalAmount);
        Sales sales=new Sales();
        sales.setSalesAmount(salesDto.getSalesAmount());
        sales.setEmployeeId(salesDto.getEmployeeId());
        sales.setStockId(resultList);
        sales.setSaleDate(salesDto.getSaleDate());
        java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(sales.getSaleDate());
        Integer[] stockArray = sales.getStockId().toArray(new Integer[0]);
        String query="INSERT INTO sales (sales_amount, stock, date, employee_id) VALUES (?,?,?,?)";
        int update = postJdbcTemplate.update(query, sales.getSalesAmount(), stockArray, timestamp, sales.getEmployeeId());
        return sales;
    }


}