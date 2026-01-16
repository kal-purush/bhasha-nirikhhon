package com.examen.zeuz.application.service;

import com.examen.zeuz.domain.model.WorkedHoursSummaryModel;
import com.examen.zeuz.domain.ports.in.GetWorkedHoursSummaryUseCase;
import com.examen.zeuz.domain.ports.out.WorkedHoursSummaryRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDate;

@Service
public class WorkedHoursSummaryService implements GetWorkedHoursSummaryUseCase {
    @Autowired
    private WorkedHoursSummaryRepository repository;

    @Override
    public WorkedHoursSummaryModel getWorkedHours(Integer employeeId, LocalDate startDate, LocalDate endDate) {
        if (!repository.employeeExists(employeeId)) {
            return new WorkedHoursSummaryModel(null, false);
        }

        if (startDate.isAfter(endDate)) {
            return new WorkedHoursSummaryModel(null, false);
        }

        Integer total = repository.getTotalWorkedHours(employeeId, startDate, endDate);
        return new WorkedHoursSummaryModel(total, total != null);
    }
}