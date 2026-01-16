package by.ras;

import by.ras.entity.particular.Product;
import by.ras.exception.ServiceException;

import java.util.List;

public interface ProductService {

    Product add(Product product) throws ServiceException;

    Product findByModel(String model) throws ServiceException;
    Product findById(long id) throws ServiceException;
    List<Product> findAll() throws ServiceException;

    Product update(Product user) throws ServiceException;

    void delete(long id) throws ServiceException;
}