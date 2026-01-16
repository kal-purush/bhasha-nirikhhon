package com.bridgelabz;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class EmployeePayrollFileIOService {
    public static String PAYROLL_FILE_NAME = "payroll.txt";
    public void writeData(List<EmployeePayrollData> employeePayrollDataList){
        StringBuffer employeeBuffer = new StringBuffer();
        employeePayrollDataList.forEach(employee -> {
            String employeeDataString = employee.toString().concat("\n");
            employeeBuffer.append(employeeDataString);
        });
        try{
            Files.write(Paths.get(PAYROLL_FILE_NAME), employeeBuffer.toString().getBytes());
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    public void printData(){
        try {
            Files.lines(new File("payroll.txt").toPath()).forEach(System.out::println);
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    public long countEntries(){
        long entries = 0;
        try {
            entries = Files.lines(new File("payroll.txt").toPath()).count();
        } catch (IOException e){
            e.printStackTrace();
        }
        return entries;
    }

    public void readData() {
    }
}