using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

/* Payroll System - Commission Employee
 * Miranda Motter
 * 28 Oct 2016
 * Creates a subclass of Employee for commissioned
 * Professor Masterson, CS 240-1
 */

namespace PayrollSystem
{
	class CommEmployee : Employee
	{
		private double commRate { get; set; }

		// Calculate payment from rate and sales
		private double calcPayment(double rate, double salesAmount)
		{
			double payment = commRate * salesAmount;
			return payment;
		}

		//private Payment payComm (double payment, Payment p)
		//{
		//    return p;
		//}
	}
}