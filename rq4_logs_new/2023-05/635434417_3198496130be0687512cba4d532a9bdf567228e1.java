package com.statista.code.challenge.dal;

import com.statista.code.challenge.models.Department;
import com.statista.code.challenge.models.CrossfitDepartment;
import com.statista.code.challenge.models.TriathlonDepartment;
import org.springframework.stereotype.Repository;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Repository
public class DepartmentsRepository {
    Map<String, Department> departments = Stream.of(new TriathlonDepartment("trainings_department"),new CrossfitDepartment("sales_deparment")).collect(Collectors.toMap(Department::getName, Function.identity()));
}