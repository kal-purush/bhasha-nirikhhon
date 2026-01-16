using System;

namespace ClassLibrary
{
    public class clsStaff
    {
        //Class Attributes
        private int staffID;
        private string staffName;
        private string staffEmail;
        private string staffPhone;
        private DateTime staffHireDate;
        private bool staffIsAdmin;
        private float staffSalary;

        //Set and Get Methods

        public int StaffID
        {
            get { return staffID; }
            set { staffID = value; }

        }
        public string StaffName
        {
            get { return staffName; }
            set { staffName = value; }
        }
        public string StaffEmail
        {
            get { return staffEmail; }
            set { staffEmail = value; }
        }
        public string StaffPhone
        {
            get { return staffPhone; }
            set { staffPhone = value; }
        }
        public DateTime StaffHireDate
        {
            get { return staffHireDate;}
            set { staffHireDate = value;}
        }
        public float StaffSalary
        {
            get { return staffSalary; }
            set { staffSalary = value; }
        }

    }
}