using System;
using System.Collections.Generic;

namespace ClassLibrary
{
    public class clsStaffCollection
    {
        List<clsStaff> mStaffList = new List<clsStaff>();

        //constructor for the class
        public clsStaffCollection()
        {
            Int32 Index = 0;
            Int32 RecordCount = 0;
            clsDataConnection DB = new clsDataConnection();
            DB.Execute("sproc_tblStaff_SelectAll");
            RecordCount = DB.Count;
            while (Index < RecordCount)
            {
                clsStaff StaffMember = new clsStaff();
                StaffMember.Name = Convert.ToString(DB.DataTable.Rows[Index]["Name"]);
                StaffMember.Email = Convert.ToString(DB.DataTable.Rows[Index]["Email"]);
                StaffMember.DateOfBirth = Convert.ToDateTime(DB.DataTable.Rows[Index]["DateOfBirth"]);
                StaffMember.Role = Convert.ToString(DB.DataTable.Rows[Index]["Role"]);
                StaffMember.HourlyWage = Convert.ToInt32(DB.DataTable.Rows[Index]["HourlyWage"]);
                StaffMember.HolidayStatus = Convert.ToBoolean(DB.DataTable.Rows[Index]["HolidayStatus"]);
                mStaffList.Add(StaffMember);
                Index++;
            }
        }

        public List<clsStaff> StaffList
        {
            get
            {
                return mStaffList;
            }
            set
            {
                mStaffList = value;
            }
        }
        public int Count
        {
            get
            {
                return mStaffList.Count;
            }
            set
            {
                
            }
        }
        public clsStaff ThisStaffMember { get; set; }
    }
}