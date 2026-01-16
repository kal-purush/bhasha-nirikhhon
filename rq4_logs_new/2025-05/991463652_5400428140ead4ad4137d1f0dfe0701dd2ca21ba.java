package com.examen.zeuz.application.service;

import com.examen.zeuz.domain.model.WorkedHoursModel;
import com.examen.zeuz.domain.ports.in.RegisterWorkedHoursUseCase;
import com.examen.zeuz.domain.ports.out.WorkedHoursRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDate;

@Service
public class WorkedHoursService implements RegisterWorkedHoursUseCase {
    @Autowired
    private WorkedHoursRepository repository;

    @Override
    public Integer register(WorkedHoursModel wh) {
        if (!repository.employeeExists(wh.getEmployeeId())) {
            return null;
        }

        if (wh.getWorkedHours() <= 0 || wh.getWorkedHours() > 20) {
            return null;
        }

        if (wh.getWorkedDate().isAfter(LocalDate.now())) {
            return null;
        }

        if (repository.workedDateExists(wh.getEmployeeId(), wh.getWorkedDate())) {
            return null;
        }

        return repository.save(wh);
    }
}