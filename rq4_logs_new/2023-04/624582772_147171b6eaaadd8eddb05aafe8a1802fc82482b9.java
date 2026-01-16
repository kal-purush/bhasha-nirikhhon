package pl.dchruscinski.adapter;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import pl.dchruscinski.entity.Customer;
import pl.dchruscinski.repository.CustomerRepository;

@Repository
public interface AdapterCustomerRepository extends CustomerRepository, JpaRepository<Customer, Integer> {

    @Override
    @Query(nativeQuery = true, value = "SELECT COUNT(*) > 0 FROM customer WHERE id = :id")
    boolean existsById(@Param("id") Integer id);

    @Override
    @Query(nativeQuery = true, value = "SELECT COUNT(*) FROM customer")
    int countCustomers();
}