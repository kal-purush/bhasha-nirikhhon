package uz.pdp.simline.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import uz.pdp.simline.entity.Employee;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface EmployeeRepository extends JpaRepository<Employee, UUID> {
    List<Employee> findAllByRole(String role);
    Optional<Employee> findByPhoneNumber(String phoneNumber);
    Optional<Employee> findByEmail(String email);
    Optional<Employee> findByName(String name);
    Optional<Employee> findBySurname(String surname);
    Optional<Employee> findBySalary(Double salary);
    void deleteAllByRole(String role);
    void deleteByPhoneNumber(String phoneNumber);
    void deleteByEmail(String email);
    void deleteByName(String name);
    void deleteBySurname(String surname);
    void deleteBySalary(Double salary);
    }