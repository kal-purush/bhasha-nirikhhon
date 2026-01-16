package ru.aston.tarakanov_aa.task4.Service;

import java.util.List;

import ru.aston.tarakanov_aa.task4.Dao.OrderDao;
import ru.aston.tarakanov_aa.task4.Entity.Order;

public class OrderDaoService {
	
	private OrderDao orderDao;
	
	public OrderDaoService(OrderDao orderDao) {
		this.orderDao = orderDao;
	}
	
	public List<Order> getOddOrders() {
		List<Order> orderList = orderDao.findAll();
		return orderList.stream().filter((x) -> x.getId()%2 != 0).toList();
	}
	
	public String getProductNameById(long id) {
		return orderDao.findEntityById(id).getProduct();
	}
	
	public boolean deleteOrdersByName(String name) {
		List<Order> orderList = orderDao.findAll();
		List<Long> idList = orderList.stream()
								.filter((x) -> x.getProduct().equals(name))
								.map((x) -> x.getId()).toList();
		
		for(Long id:idList) {
			if(!orderDao.delete(id.longValue())) {
				return false;
			}
		}
		return true;
	}
	
	public List<Order> setPriceForGroupOrders(double price, List<Order> orderList) {
		return orderList.stream().map(orderDao::update).toList();		
	}
}