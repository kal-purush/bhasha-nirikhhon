package com.doubleo.employeeservice.domain.department.domain;

import com.doubleo.employeeservice.domain.common.model.BaseTimeEntity;
import com.doubleo.employeeservice.domain.employee.domain.Employee;
import jakarta.persistence.*;
import java.util.ArrayList;
import lombok.*;

@Entity
@Getter
@NoArgsConstructor
public class Department extends BaseTimeEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "department_id")
    private Long id;

    @Column(name = "department_name")
    private String departmentName; // 정규화 추가

    @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.LAZY, orphanRemoval = true)
    private ArrayList<Employee> employeeList = new ArrayList<>();

    @Builder(access = AccessLevel.PRIVATE)
    private Department(String departmentName) {
        this.departmentName = departmentName;
    }

    public static Department createDepartment(String departmentName) {
        return Department.builder().departmentName(departmentName).build();
    }
}