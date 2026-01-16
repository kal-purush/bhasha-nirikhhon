package com.es.core.dao;

import com.es.core.model.order.Order;
import com.es.core.model.order.OrderStatus;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class OrderRowMapper implements RowMapper<Order> {
    @Override
    public Order mapRow(ResultSet resultSet, int i) throws SQLException {
        Order order = new Order();
        setFieldValues(order, resultSet);
        return order;
    }

    private void setFieldValues(Order order, ResultSet resultSet) throws SQLException, DataAccessException {
        order.setId(resultSet.getLong("id"));
        order.setSecureId(resultSet.getString("secureId"));
        order.setSubtotal(resultSet.getBigDecimal("subtotal"));
        order.setDeliveryPrice(resultSet.getBigDecimal("deliveryPrice"));
        order.setTotalPrice(resultSet.getBigDecimal("totalPrice"));
        order.setFirstName(resultSet.getString("firstName"));
        order.setLastName(resultSet.getString("lastName"));
        order.setDeliveryAddress(resultSet.getString("deliveryAddress"));
        order.setContactPhoneNo(resultSet.getString("contactPhoneNo"));
        order.setAdditionalInformation(resultSet.getString("additionalInformation"));
        order.setStatus(OrderStatus.valueOf(resultSet.getString("status")));
        order.setOrderDate(resultSet.getTimestamp("orderDate").toLocalDateTime());
    }
}