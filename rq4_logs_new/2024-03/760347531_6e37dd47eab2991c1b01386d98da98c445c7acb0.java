package lk.zerocode.api.service.impl;

import lk.zerocode.api.controller.request.AttendenceRequest;
import lk.zerocode.api.controller.request.Testrq;
import lk.zerocode.api.exceptions.AttendanceException;
import lk.zerocode.api.model.Employee;
import lk.zerocode.api.model.FingerPrint;
import lk.zerocode.api.model.Test;
import lk.zerocode.api.proj.AttendanceCountProjection;
import lk.zerocode.api.repository.AttendenceRepository;
import lk.zerocode.api.repository.EmployeeRepository;
import lk.zerocode.api.repository.FingerPrintRepository;
import lk.zerocode.api.repository.TestRepo;
import lk.zerocode.api.service.TestService;
import lombok.AllArgsConstructor;
import org.modelmapper.ModelMapper;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@AllArgsConstructor
public class TestServiceImpl implements TestService {

    private TestRepo testRepo;
    private FingerPrintRepository fingerPrintRepository;
    private EmployeeRepository employeeRepository;
    private AttendenceRepository attendenceRepository;
    @Override
    public void saveDate(Testrq testrq) {

        Test test = new Test();
        test.setDate(testrq.getDate());
        test.setEmpId(testrq.getEmpId());

        testRepo.save(test);
    }

    @Override
    public void getAttCount(AttendenceRequest attendenceRequest) throws AttendanceException {
        String fid = attendenceRequest.getFingerPrintId();
        System.out.println(fid);

        FingerPrint fingerPrint = fingerPrintRepository.findByFingerPrintId(fid)
                .orElseThrow(() -> new AttendanceException("Employee fingerprint not found!"));
        Employee employee = employeeRepository.findById(fingerPrint.getEmployee().getId()).orElseThrow(
                () -> new AttendanceException("employee not found!")
        );

        System.out.println(employee.getId());

        Integer count = attendenceRepository.getAttendanceCount(employee.getId());
        System.out.println("Count = " + count);

        if (count >= 2){
            System.out.println("aye not covering be");
        }else {
            System.out.println("not covering puluwn");
        }
    }
}