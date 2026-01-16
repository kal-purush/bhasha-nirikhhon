package com.examen.zeuz.application.service;

import com.examen.zeuz.domain.model.EmployeePaymentSummaryModel;
import com.examen.zeuz.domain.ports.in.GetEmployeePaymentUseCase;
import com.examen.zeuz.domain.ports.out.EmployeePaymentRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDate;

@Service
public class EmployeePaymentService implements GetEmployeePaymentUseCase {
    @Autowired
    private EmployeePaymentRepository repository;

    @Override
    public EmployeePaymentSummaryModel getPayment(Integer employeeId, LocalDate startDate, LocalDate endDate) {
        if (!repository.employeeExists(employeeId)) {
            return new EmployeePaymentSummaryModel(null, false);
        }

        if (startDate.isAfter(endDate)) {
            return new EmployeePaymentSummaryModel(null, false);
        }

        Integer payment = repository.getPayment(employeeId, startDate, endDate);
        return new EmployeePaymentSummaryModel(payment, payment != null);
    }
}