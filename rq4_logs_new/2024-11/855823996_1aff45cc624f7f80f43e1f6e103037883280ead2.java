/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 */

package com.mycompany.employeeseries;
import com.mycompany.employeeseries.version1.*;

/**
 *
 * @author User
 */
public class EmployeeSeries {

    public static void main(String[] args) {
            
            System.out.println("-----HOURLY EMPLOYEE-----");
            HourlyEmployee hr1 = new HourlyEmployee();
            hr1.setEmpID(1000);
            hr1.setEmpName("Josh");
            hr1.setRatePerHour(50);
            hr1.setTotalHoursWorked(39);
            
            HourlyEmployee hr2 = new HourlyEmployee("Jio", 1001);
            hr2.setRatePerHour(100.00);
            hr2.setTotalHoursWorked(20);
            
            HourlyEmployee hr3 = new HourlyEmployee(41, 100, "Jansen", 1002);
            
            hr1.displayHourlyEmployee();
            hr2.displayHourlyEmployee();
            hr3.displayHourlyEmployee();
                
            System.out.println("\n-----PIECE WORKER EMPLOYEE-----");
            PieceWorkerEmployee pr1 = new PieceWorkerEmployee();
            
            pr1.setEmpID(2000);
            pr1.setEmpName("Gayle");
            pr1.setRatePerPiece(500);
            pr1.setTotalPiecesFinished(50);
            
            PieceWorkerEmployee pr2 = new PieceWorkerEmployee("Granito", 2001);
            pr2.setRatePerPiece(400);
            pr2.setTotalPiecesFinished(200);
            
            PieceWorkerEmployee pr3 = new PieceWorkerEmployee(300, 100, "Dusky", 2002);
            
            pr1.displayHourlyEmployee();
            pr2.displayHourlyEmployee();
            pr3.displayHourlyEmployee();

            System.out.println("\n-----COMMISSION EMPLOYEE-----");
            CommissionEmployee ce1 = new CommissionEmployee();
            ce1.setEmpID(3000);
            ce1.setEmpName("Summi");
            ce1.setTotalSales(5000);
            
            CommissionEmployee ce2 = new CommissionEmployee("Yousif", 3001);
            ce2.setTotalSales(50000);
            
            CommissionEmployee ce3 = new CommissionEmployee(100000, "Gran", 3002);
            
            ce1.displayHourlyEmployee();
            ce2.displayHourlyEmployee();
            ce3.displayHourlyEmployee();

            System.out.println("\n-----BASE PLUS COMMISSION EMPLOYEE-----");
            BasePlusCommissionEmployee bpce1 = new BasePlusCommissionEmployee();
            bpce1.setEmpID(4000);
            bpce1.setEmpName("John");
            bpce1.setBaseSalary(550);
            bpce1.setTotalSales(50000);
            
            BasePlusCommissionEmployee bpce2 = new BasePlusCommissionEmployee("Kaw", 4001);
            bpce2.setBaseSalary(450);
            bpce2.setTotalSales(150000);
            
            BasePlusCommissionEmployee bpce3 = new BasePlusCommissionEmployee(100000, 500, "Hans", 4002);
            bpce1.displayHourlyEmployee();
            bpce2.displayHourlyEmployee();
            bpce3.displayHourlyEmployee();
    }
}