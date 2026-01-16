package by.ras.impl;

import by.ras.OrderService;
import by.ras.entity.particular.Order;
import by.ras.exception.ServiceException;
import by.ras.repository.OrderRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class OrderServiceImpl implements OrderService {

    private final
    OrderRepository orderRepository;

    @Autowired
    public OrderServiceImpl(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    @Override
    public Order addOrder(Order order) throws ServiceException {
        try {
            return orderRepository.saveAndFlush(order);
        }catch (Exception e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public Order findById(long id) throws ServiceException {
        try {
            return orderRepository.findOne(id);
        }catch (Exception e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public List<Order> getAll() throws ServiceException {
        try {
            return orderRepository.findAll();
        }catch (Exception e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public Order editOrder(Order order) throws ServiceException {
        try {
            return orderRepository.saveAndFlush(order);
        }catch (Exception e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public void delete(long id) throws ServiceException {
        try {
            orderRepository.delete(id);
        }catch (Exception e) {
            throw new ServiceException(e);
        }
    }
}