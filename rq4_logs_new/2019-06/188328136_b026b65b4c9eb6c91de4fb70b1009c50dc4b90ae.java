package com.macoredroid.onlinebookstore.serviceimpl;

import com.macoredroid.onlinebookstore.dao.BooklistDao;
import com.macoredroid.onlinebookstore.dao.OrderDao;
import com.macoredroid.onlinebookstore.dao.UserDao;
import com.macoredroid.onlinebookstore.entity.Booklist;
import com.macoredroid.onlinebookstore.entity.Order;
import com.macoredroid.onlinebookstore.entity.User;
import com.macoredroid.onlinebookstore.service.DirectlyOrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DirectlyOrderServiceimpl implements DirectlyOrderService {
    @Autowired
    BooklistDao booklistDao;
    @Autowired
    OrderDao orderDao;
    @Autowired
    UserDao userDao;
    @Override
    public boolean DirectlyOrder(String username, String isbn, String number, String time) {
        Booklist tempBook=booklistDao.findByIsbn(isbn);
        User tempUser=userDao.findOne(username);
        if(tempBook==null)
        {
            return false;
        }
        if(tempUser==null)
        {
            return false;
        }
        int tempOrdernum = Integer.parseInt(number);
        int tempStocknum = tempBook.getStock();
        if (tempStocknum < tempOrdernum)
        {
            return false;
        }
        else
        {
            tempBook.setStock(tempStocknum-tempOrdernum);
            booklistDao.save(tempBook);
            Order temporder=new Order(isbn,time,tempOrdernum,tempUser);
            List<Order> tempOrderList=tempUser.getOrders();
            tempOrderList.add(temporder);
            tempUser.setOrders(tempOrderList);
            userDao.save(tempUser);
            orderDao.save(temporder);
            return true;
        }

    }
}