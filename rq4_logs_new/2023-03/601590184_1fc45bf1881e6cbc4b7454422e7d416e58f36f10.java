package com.ua.robot.lesson13work;

import com.ua.robot.lesson13work.domain.Employee;
import com.ua.robot.lesson13work.repository.EmployeeMemoryRepository;

public class Main {
    public static void main(String[] args) {
        EmployeeMemoryRepository repository = new EmployeeMemoryRepository();

        Employee[] employees = repository.findAll();
        for (Employee employee : employees) {
            if (employee != null) {
                System.out.println(employee);
            }

        }

    }
}