package ebhor.action;

import ebhor.dao.EmployeeDao;
import java.util.List;
import ebhor.model.Employee;
import java.math.BigDecimal;
import java.sql.Date;

/**
 * All data accept and manipulation should be done here
 *
 * @author Manish Sahu
 */
public class EmployeeAction {

    EmployeeDao dao = new EmployeeDao();
    int st;

    public void insert(Employee employee) {
        st = dao.insert(employee);
        if (st == 1) {
            System.out.println("Employee Inserted Successfully");
        } else if (st == -1) {
            System.out.println("Employee Already exists");
        } else {
            System.out.println("Unable to Insert Employee");
        }
    }

    public void update(Employee employee) {
        st = dao.update(employee);
        if (st == 1) {
            System.out.println("Employee Updated Successfully");
        } else {
            System.out.println("Unable to update Employee");
        }
    }

    public void delete(Long id) {
        st = dao.delete(id);
        if (st == 1) {
            System.out.println("Employee Deleted Successfully");
        } else {
            System.out.println("Unable to Delete Employee");
        }
    }

    public void fetchById(Long id) {
        Employee employee = dao.fetchById(id);
        System.out.println("Employee Details are :");
        System.out.println(employee);
    }

    public void fetchByEmailId(String emailId) {
        Employee employee = dao.fetchByEmailId(emailId);
        System.out.println("Employee Details are :");
        System.out.println(employee);
    }

    public void fetchByMobileNo(String mobileNo) {
        Employee employee = dao.fetchByMobileNo(mobileNo);
        System.out.println("Employee Details are :");
        System.out.println(employee);
    }

    public void searchByName(String name) {
        List<Employee> employeeList = dao.searchByName(name);
        System.out.println("Employee Details are :");
        for (Employee employee : employeeList) {
            System.out.println(employee);
        }
    }

    public void fetchByCity(String city) {
        List<Employee> employeeList = dao.fetchByCity(city);
        System.out.println("Employee Details are :");
        for (Employee employee : employeeList) {
            System.out.println(employee);
        }
    }

    public void fetchBySalaryRange(BigDecimal lowerSalary, BigDecimal higherSalary) {
        List<Employee> employeeList = dao.fetchBySalaryRange(lowerSalary, higherSalary);
        System.out.println("Employee Details are :");
        for (Employee employee : employeeList) {
            System.out.println(employee);
        }
    }

    public void fetchByDob(Date dob) {
        List<Employee> employeeList = dao.fetchByDob(dob);
        System.out.println("Employee Details are :");
        for (Employee employee : employeeList) {
            System.out.println(employee);
        }
    }

    public void fetchByRangeDoj(Date startDate, Date endDate) {
        List<Employee> employeeList = dao.fetchByRangeDoj(startDate,endDate);
        System.out.println("Employee Details are :");
        for (Employee employee : employeeList) {
            System.out.println(employee);
        }
    }

    public void fetchAll() {
        List<Employee> employeeList = dao.fetchAll();
        System.out.println("Employee Details are :");
        for (Employee employee : employeeList) {
            System.out.println(employee);
        }
    }

}