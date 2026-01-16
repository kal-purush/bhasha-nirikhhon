/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package employeeapp;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 *
 * @author catellez
 */
public class EmpList implements Serializable {
    
    
    
    public EmpList(){
        emp = new ArrayList<Employee>();
        
        
    }
    
    public void addEmp(Employee e)
    {
        emp.add(e);
        
    }
    
    public String printAllEmp()
    {
        for (Employee e: emp)
            
            return e.toString();
        
    }
    
     
     
     public String printInOrderNames(Comparator<Employee> emp)
     {
         Collections.sort(emp, Employee.createComparatorByName(true));
         
         return emp.toString();
     }
     
       public static Comparator<Employee> createComparatorBySalary(final boolean increasing){
       return new
           Comparator<Employee>(){
               public int compare(Employee e, Employee m){
                   if (increasing){
                       
                   if (e.getSalary() > m.getSalary()){
                       return 1;
                   }
                   else if (e.getSalary() < m.getSalary()){
                       return -1;
                   }
                   else{
                       return 0;
                   }
                   
                   }
                   else{
                       if (e.getSalary() < m.getSalary()){
                       return 1;
                   }
                   else if (e.getSalary() > m.getSalary()){
                       return -1;
                   }
                   else{
                       return 0;
                   }
                   }
               }
           };
   }
    
  public String printInOrderSalary(Comparator<Employee> emp)
     {
         Collections.sort(emp, createComparatorBySalary(true));
         
         return emp.toString();
     }
    
   public String printAllManagers(ArrayList<Employee> emp)
   {
        ArrayList managers = new ArrayList<Manager>();
        for (Employee e: emp)
            if (e.getClass() = Manager)
                managers.add(e);
        return managers.toString();
        
   }
   
    public String printAllHourly(ArrayList<Employee> emp)
   
    {
        ArrayList hourly = new ArrayList<Hourly>();
        for (Employee e: emp)
            if (e.getClass() = Hourly)
                hourly.add(e);
        return hourly.toString();
        
   }
    
    public String printAllSalaried(ArrayList<Employee> emp)
   
    {
        ArrayList salaried = new ArrayList<Salaried>();
        for (Employee e: emp)
            if (e.getClass() = Salaried)
                salaried.add(e);
        return salaried.toString();
        
   }
    
    public void changeOvertime(ArrayList<Employee> fullList, int newRate)
    {
        for (Employee e: emp)
            if (e.getClass() = Salaried)
                e.setOverTimeRate(newRate);
    }
    
    public void changeHourly(ArrayList<Employee> fullList, int newRate)
    {
        for (Employee e: emp)
            if (e.getClass() = Hourly)
                e.setHourlyRate(newRate);
    }
 
    private ArrayList<Employee> emp;
}