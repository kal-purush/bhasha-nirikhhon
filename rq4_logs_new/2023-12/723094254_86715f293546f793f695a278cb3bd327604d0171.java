package com.psp.possystem;

import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.tags.Tag;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.psp.possystem.common.Exceptions;
import com.psp.possystem.dto.Employee;
import com.psp.possystem.query.QueryResponse;
import com.psp.possystem.query.QuerySorting;

@RestController
@RequestMapping("/api/employees")
@Tag(name = "Employee")
public class EmployeeController {

    // 1. POST /employees/
    @RequestMapping(value = "/", method = RequestMethod.POST)
    @ResponseBody
    public Employee create(@RequestBody Employee employee) {
        throw Exceptions.NotImplementedException;
    }

    // 2. GET /employees/{employeeId}
    @RequestMapping(value = "/{employeeId}", method = RequestMethod.GET)
    @ResponseBody
    public Employee get(@PathVariable int employeeId) {
        throw Exceptions.NotImplementedException;
    }

    // 3. GET /orders/query
    /**
     * @param sorting        - unordered by default
     * @param limit          - maximum number of entities to return
     * @param offset         - starting point of the result set
     * @param ofCustomers    - if not specified all employees with specified
     *                       customerIds are queried, if specified but empty - none.
     * @param ofPhoneNumbers - if not specified all employees with specified
     *                       phone numbers are queried, if specified but empty -
     *                       none.
     * @param ofEmails       - if not specified all employees with specified
     *                       emails are queried, if specified but empty - none.
     */
    @RequestMapping(value = "/query", method = RequestMethod.GET)
    @ResponseBody
    public QueryResponse<Employee> query(
            @RequestParam(defaultValue = "UNORDERED") QuerySorting.Sorting sorting,
            @RequestParam(defaultValue = "100") int limit,
            @RequestParam(defaultValue = "100") int offset,
            @RequestParam Optional<List<String>> ofEmails,
            @RequestParam Optional<List<String>> ofPhoneNumbers,
            @RequestParam Optional<List<String>> ofCustomers) {
        throw Exceptions.NotImplementedException;
    }

    // 4. DELETE /employees/{employee id}
    @RequestMapping(value = "/{employee_id}", method = RequestMethod.DELETE)
    @ResponseBody
    public void delete(@PathVariable int employee_id) {
        throw Exceptions.NotImplementedException;
    }

    // 5. GET /employees/{employee_id}/tips - tips of an employee
    @RequestMapping(value = "/{employee_id}/tips", method = RequestMethod.GET)
    @ResponseBody
    public int tips(@PathVariable int employee_id) {
        throw Exceptions.NotImplementedException;
    }

    // 6. PATCH /employees/permit - add a permission to an employee

    /**
     * @param employee_id      - employee Id.
     * @param permission_name  - name of the permission to give
     */
    @RequestMapping(value = "/{employee_id}/permit/{permission_name}", method = RequestMethod.GET)
    @ResponseBody
    public void permit(@PathVariable int employee_id, @PathVariable String permission_name) {
        throw Exceptions.NotImplementedException;
    }

    // 7. PATCH /employees/unpermit - remove a permission from an employee
    /**
     * @param employee_id      - employee Id.
     * @param permission_name  - name of the permission to give
     */
    @RequestMapping(value = "/{employee_id}/unpermit/{permission_name}", method = RequestMethod.GET)
    @ResponseBody
    public void unpermit(@PathVariable int employee_id, @PathVariable String permission_name) {
        throw Exceptions.NotImplementedException;
    }
}