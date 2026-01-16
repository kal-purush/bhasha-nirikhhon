package com.examen.zeuz.application.service;

import com.examen.zeuz.domain.model.EmployeeModel;
import com.examen.zeuz.domain.ports.in.CreateEmployeeUseCase;
import com.examen.zeuz.domain.ports.out.EmployeeRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.Period;

@Service
public class EmployeeService implements CreateEmployeeUseCase {
    @Autowired
    private EmployeeRepository repository;

    @Override
    public Integer createEmployee(EmployeeModel employee){
        if (repository.existsByNameAndLastName(employee.getName(), employee.getLastName())){
            return null;
        }

        if (!isAdult(employee.getBirthdate())) {
            return null;
        }

        if (!repository.genderExists((employee.getGenderId())) ||
            !repository.jobExists((employee.getJobId()))) {
            return null;
        }

        return repository.save(employee);
    }
    private boolean isAdult(LocalDate birthdate) {
        return Period.between(birthdate, LocalDate.now()).getYears() >= 18;
    }
}