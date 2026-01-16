package com.examen.zeuz.application.service;

import com.examen.zeuz.domain.model.EmployeeWithDetailsModel;
import com.examen.zeuz.domain.ports.in.GetEmployeesByJobUseCase;
import com.examen.zeuz.domain.ports.out.EmployeeQueryRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class EmployeeQueryService implements GetEmployeesByJobUseCase {
    @Autowired
    private EmployeeQueryRepository repository;

    @Override
    public List<EmployeeWithDetailsModel> getEmployeesByJobId(Integer jobId) {
        if (!repository.jobExists(jobId)) {
            return null;
        }
        return repository.findByJobId(jobId);
    }
}