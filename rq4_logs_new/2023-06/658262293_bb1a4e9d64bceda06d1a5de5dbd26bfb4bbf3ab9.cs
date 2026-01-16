using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Day1_EmpWage
{
    public class CalWageUsingConstructor
    {
        int EmpHrs = 0, TotalEmpWage = 0, TotalEmpHrs = 0;
        int TotalWorkingDays = 0;
        int Emp_Rate_Per_Hour = 20;
        int WORKING_DAYS;
        int MAX_HRS_IN_MONTH;
        public CalWageUsingConstructor(int WORKING_DAYS, int MAX_HRS_IN_MONTH)
        {
           this.WORKING_DAYS = WORKING_DAYS;
          this.MAX_HRS_IN_MONTH = MAX_HRS_IN_MONTH;
        }
        public const int Is_Full_Time = 1;
        public const int Is_Part_Time = 2;
        public void CalWage_Of_Employee()
        {
            while (TotalEmpHrs <= MAX_HRS_IN_MONTH && TotalWorkingDays < WORKING_DAYS)
            {
                TotalWorkingDays++;
                Random random = new Random();
                int EmpCheck = (random.Next(3));
                switch (EmpCheck)
                {
                    case Is_Full_Time:
                        EmpHrs = 8;
                        break;

                    case Is_Part_Time:
                        EmpHrs = 4;
                        break;
                    default:
                        EmpHrs = 0;
                        break;
                }
                TotalEmpHrs += EmpHrs;
                Console.WriteLine(" Days :" + TotalWorkingDays + " Emp Hours :" + EmpHrs);
            }
            TotalEmpWage = Emp_Rate_Per_Hour * TotalEmpHrs;
            Console.WriteLine(" Employee Total Wage :" + TotalEmpWage);
        }
    }
}