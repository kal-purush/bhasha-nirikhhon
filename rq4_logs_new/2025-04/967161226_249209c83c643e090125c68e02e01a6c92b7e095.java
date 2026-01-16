package re.edu.business.dao.customer;

import re.edu.business.dao.AppDao;
import re.edu.business.model.customer.Customer;

import java.util.List;

public interface CustomerDao extends AppDao<Customer> {
    List<Customer> findAll();
    boolean addNewCustomer(Customer customer);
    boolean updateCustomer(Customer customer);
    Customer findCustomerById(int id);
    boolean deleteCustomer(Customer customer);
    boolean isPhoneDuplicate(String phone);
    boolean isEmailDuplicate(String email);
    List<Customer> searchCustomerByName (String name);
    List<Customer> searchCustomerByEmail (String email);
}