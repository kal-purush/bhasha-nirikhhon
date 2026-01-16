package com.silverbars;

import com.silverbars.exception.OrderNotException;
import com.silverbars.model.Order;
import com.silverbars.model.SummaryBuilder;

import java.util.List;

public interface LiveOrderBoard {

    void orderRegisteration(Order order);

    void cancelOrder(Order order);

    List<SummaryBuilder> orderSummarySelection() throws OrderNotException;
}