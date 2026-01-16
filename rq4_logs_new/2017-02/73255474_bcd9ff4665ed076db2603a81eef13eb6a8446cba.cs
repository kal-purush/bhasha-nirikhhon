using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace Lakshya_Yatra
{
    public class FormFactory
    {
        private FormFactory()
        { }

        private static FormFactory _Instance = null;

        private SearchCustomer frmSearchCustomer = null;
        private Multiple_Registration frmMultipleRegistration = null;
        private BusRegistration frmBusRegistration = null;

        private DeleteAllDataPwd frmDeleteData = null;
        private BusReport frmBusReport = null;
        private EmptySeats frmEmptySeats = null;
        private CashExpenses frmCashExpenses = null;
        DailyCashReport frmDailyCashReport = null;
        Report frmReport = null;
        AllYatraDatesAllBusesReport frmAllYatraDatesAllBusesReport = null;
        UserMaintenance frmUserMaintenance = null;
        ChangePassword frmChangePassword = null;
        DiscountReport frmDiscountReport = null;
        BloodGroupReport frmBloodGroupReport = null;
        UserwiseCashReport frmUserwiseCashReport = null;
        UserwiseYearlyCashReport frmUserwiseYearlyCashReport = null;
        ExpenseReport frmExpenseReport = null;
        BirthDateReport frmBirthDateReport = null;
        BirthdayWishes frmBirthdayWishes = null;
        CustomSMS frmCustomSMS = null;
        UserwiseTicketReport frmUserwiseTicketReport = null;
        ElectionReport frmElectionReport = null;
        CustomerMaintenance frmCustomerMaintenance = null;
        AreaMaintenance frmAreaMaintenance = null;

        public static FormFactory Instance
        {
            get
            {
                if (_Instance == null)
                    _Instance = new FormFactory();
                return _Instance;
            }
        }

        public Form CreateOrActivateForm(string formName)
        {
            Form f = null;
            switch (formName)
            {
                case "SearchCustomer":
                    {
                        if (frmSearchCustomer == null)
                        {
                            frmSearchCustomer = new SearchCustomer();
                            frmSearchCustomer.FormClosing += FormClosing;
                        }
                        f = frmSearchCustomer;
                        break;
                    }
                case "Multiple Registration":
                    {
                        if (frmMultipleRegistration == null)
                        {
                            frmMultipleRegistration = new Multiple_Registration();
                            frmMultipleRegistration.FormClosing += FormClosing;
                        }
                        f = frmMultipleRegistration;
                        break;
                    }
                case "Bus Registration":
                    {
                        if (frmBusRegistration == null)
                        {
                            frmBusRegistration = new BusRegistration();
                            frmBusRegistration.FormClosing += FormClosing;
                        }
                        f = frmBusRegistration;
                        break;
                    }
                case "Delete Data":
                    {
                        if (frmDeleteData == null)
                        {
                            frmDeleteData = new DeleteAllDataPwd();
                            frmDeleteData.FormClosing += FormClosing;
                        }
                        f = frmDeleteData;
                        break;
                    }
                case "BusReport":
                    {
                        if (frmBusReport == null)
                        {
                            frmBusReport = new BusReport();
                            frmBusReport.FormClosing += FormClosing;
                        }
                        f = frmBusReport;
                        break;
                    }
                case "Empty Seats":
                    {
                        if (frmEmptySeats == null)
                        {
                            frmEmptySeats = new EmptySeats();
                            frmEmptySeats.FormClosing += FormClosing;
                        }
                        f = frmEmptySeats;
                        break;
                    }
                case "Cash Expenses":
                    {
                        if (frmCashExpenses == null)
                        {
                            frmCashExpenses = new CashExpenses();
                            frmCashExpenses.FormClosing += FormClosing;
                        }
                        f = frmCashExpenses;
                        break;
                    }
                case "Daily Cash Report":
                    {
                        if (frmDailyCashReport == null)
                        {
                            frmDailyCashReport = new DailyCashReport();
                            frmDailyCashReport.FormClosing += FormClosing;
                        }
                        f = frmDailyCashReport;
                        break;
                    }
                case "Report":
                    {
                        if (frmReport == null)
                        {
                            frmReport = new Report();
                            frmReport.FormClosing += FormClosing;
                        }
                        f = frmReport;
                        break;
                    }
                case "All Yatra Dates - All Buses Report":
                    {
                        if (frmAllYatraDatesAllBusesReport == null)
                        {
                            frmAllYatraDatesAllBusesReport = new AllYatraDatesAllBusesReport();
                            frmAllYatraDatesAllBusesReport.FormClosing += FormClosing;
                        }
                        f = frmAllYatraDatesAllBusesReport;
                        break;
                    }
                case "User Maintenance":
                    {
                        if (frmUserMaintenance == null)
                        {
                            frmUserMaintenance = new UserMaintenance();
                            frmUserMaintenance.FormClosing += FormClosing;
                        }
                        f = frmUserMaintenance;
                        break;
                    }
                case "Change Password":
                    {
                        if (frmChangePassword == null)
                        {
                            frmChangePassword = new ChangePassword();
                            frmChangePassword.FormClosing += FormClosing;
                        }
                        f = frmChangePassword;
                        break;
                    }
                case "Discount Report":
                    {
                        if (frmDiscountReport == null)
                        {
                            frmDiscountReport = new DiscountReport();
                            frmDiscountReport.FormClosing += FormClosing;
                        }
                        f = frmDiscountReport;
                        break;
                    }
                case "Blood Group Report":
                    {
                        if (frmBloodGroupReport == null)
                        {
                            frmBloodGroupReport = new BloodGroupReport();
                            frmBloodGroupReport.FormClosing += FormClosing;
                        }
                        f = frmBloodGroupReport;
                        break;
                    }
                case "Userwise Cash Report":
                    {
                        if (frmUserwiseCashReport == null)
                        {
                            frmUserwiseCashReport = new UserwiseCashReport();
                            frmUserwiseCashReport.FormClosing += FormClosing;
                        }
                        f = frmUserwiseCashReport;
                        break;
                    }
                case "Userwise Yearly Cash Report":
                    {
                        if (frmUserwiseYearlyCashReport == null)
                        {
                            frmUserwiseYearlyCashReport = new UserwiseYearlyCashReport();
                            frmUserwiseYearlyCashReport.FormClosing += FormClosing;
                        }
                        f = frmUserwiseYearlyCashReport;
                        break;
                    }
                case "Expense Report":
                    {
                        if (frmExpenseReport == null)
                        {
                            frmExpenseReport = new ExpenseReport();
                            frmExpenseReport.FormClosing += FormClosing;
                        }
                        f = frmExpenseReport;
                        break;
                    }
                case "Birth Date Report":
                    {
                        if (frmBirthDateReport == null)
                        {
                            frmBirthDateReport = new BirthDateReport();
                            frmBirthDateReport.FormClosing += FormClosing;
                        }
                        f = frmBirthDateReport;
                        break;
                    }
                case "Birthday Wishes":
                    {
                        if (frmBirthdayWishes == null)
                        {
                            frmBirthdayWishes = new BirthdayWishes();
                            frmBirthdayWishes.FormClosing += FormClosing;
                        }
                        f = frmBirthdayWishes;
                        break;
                    }
                case "Custom SMS":
                    {
                        if (frmCustomSMS == null)
                        {
                            frmCustomSMS = new CustomSMS();
                            frmCustomSMS.FormClosing += FormClosing;
                        }
                        f = frmCustomSMS;
                        break;
                    }
                case "Userwise Ticket Report":
                    {
                        if (frmUserwiseTicketReport == null)
                        {
                            frmUserwiseTicketReport = new UserwiseTicketReport();
                            frmUserwiseTicketReport.FormClosing += FormClosing;
                        }
                        f = frmUserwiseTicketReport;
                        break;
                    }
                case "Election Report":
                    {
                        if (frmElectionReport == null)
                        {
                            frmElectionReport = new ElectionReport();
                            frmElectionReport.FormClosing += FormClosing;
                        }
                        f = frmElectionReport;
                        break;
                    }
                case "Customer Maintenance":
                    {
                        if (frmCustomerMaintenance == null)
                        {
                            frmCustomerMaintenance = new CustomerMaintenance();
                            frmCustomerMaintenance.FormClosing += FormClosing;
                        }
                        f = frmCustomerMaintenance;
                        break;
                    }
                case "Area Maintenance":
                    {
                        if (frmAreaMaintenance == null)
                        {
                            frmAreaMaintenance = new AreaMaintenance();
                            frmAreaMaintenance.FormClosing += FormClosing;
                        }
                        f = frmAreaMaintenance;
                        break;
                    }

            }
            return f;
        }

        void FormClosing(object sender, FormClosingEventArgs e)
        {
            switch (sender.GetType().Name)
            {
                case "SearchCustomer":
                    frmSearchCustomer = null;
                    break;
                case "Multiple_Registration":
                    frmMultipleRegistration = null;
                    break;
                case "BusRegistration":
                    {
                        frmBusRegistration = null;
                        break;
                    }
                case "DeleteAllDataPwd":
                    {
                        frmDeleteData = null;
                        break;
                    }
                case "BusReport":
                    {
                        frmBusReport = null;
                        break;
                    }
                case "EmptySeats":
                    {
                        frmEmptySeats = null;
                        break;
                    }
                case "CashExpenses":
                    {
                        frmCashExpenses = null;
                        break;
                    }
                case "DailyCashReport":
                    {
                        frmDailyCashReport = null;
                        break;
                    }
                case "Report":
                    {
                        frmReport = null;
                        break;
                    }
                case "AllYatraDatesAllBusesReport":
                    {
                        frmAllYatraDatesAllBusesReport = null;
                        break;
                    }
                case "UserMaintenance":
                    {
                        frmUserMaintenance = null;
                        break;
                    }
                case "ChangePassword":
                    {
                        frmChangePassword = null;
                        break;
                    }
                case "DiscountReport":
                    {
                        frmDiscountReport = null;
                        break;
                    }
                case "BloodGroupReport":
                    {
                        frmBloodGroupReport = null;
                        break;
                    }
                case "UserwiseCashReport":
                    {
                        frmUserwiseCashReport = null;
                        break;
                    }
                case "UserwiseYearlyCashReport":
                    {
                        frmUserwiseYearlyCashReport = null;
                        break;
                    }
                case "ExpenseReport":
                    {
                        frmExpenseReport = null;
                        break;
                    }
                case "BirthDateReport":
                    {
                        frmBirthDateReport = null;
                        break;
                    }
                case "BirthdayWishes":
                    {
                        frmBirthdayWishes = null;
                        break;
                    }
                case "CustomSMS":
                    {
                        frmCustomSMS = null;
                        break;
                    }
                case "UserwiseTicketReport":
                    {
                        frmUserwiseTicketReport = null;
                        break;
                    }
                case "ElectionReport":
                    {
                        frmElectionReport = null;
                        break;
                    }
                case "CustomerMaintenance":
                    {
                        frmCustomerMaintenance = null;
                        break;
                    }
                case "AreaMaintenance":
                    {
                        frmAreaMaintenance = null;
                        break;
                    }
            }
        }
    }
}