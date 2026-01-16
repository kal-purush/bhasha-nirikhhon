using System;
using System.Collections.Generic;


namespace ClassLibrary
{
    public class clsStaffCollection
    {
        public clsStaffCollection()
        {
            //variable for the index
            Int32 Index = 0;
            //variable to store the record count
            Int32 RecordCount = 0;
            //object for data connect
            clsDataConnection DB = new clsDataConnection();
            //execute the stored procedure
            DB.Execute("sproc_StaffTable_SelectAll");
            //get the count of records
            RecordCount=DB.Count;
            //whilw there are records to process
            while(Index< RecordCount)
            {
                //create the items of test data
                clsStaff aStaff = new clsStaff();
                //read the fields for the current record
                aStaff.StaffID = Convert.ToInt32(DB.DataTable.Rows[Index]["Staff_Id"]);
                aStaff.StaffName = Convert.ToString(DB.DataTable.Rows[Index]["Staff_Name"]);
                aStaff.StaffEmail = Convert.ToString(DB.DataTable.Rows[Index]["Staff_Email"]);
                aStaff.StaffPhone = Convert.ToString(DB.DataTable.Rows[Index]["Staff_Phone"]);
                aStaff.StaffHireDate = Convert.ToDateTime(DB.DataTable.Rows[Index]["Staff_Hire_Date"]);
                aStaff.StaffIsAdmin = Convert.ToBoolean(DB.DataTable.Rows[Index]["Staff_Is_Admin"]);
                aStaff.StaffSalary = Convert.ToInt32(DB.DataTable.Rows[Index]["Staff_Salary"]);
                //add the record to the private data member
                mStaffList.Add(aStaff);
                //point at the next record
                Index++;
            }
            
        }
        List<clsStaff> mStaffList= new List<clsStaff>();
        public List<clsStaff>StaffList
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
                //we shall worry about this later
            }
        }
        public clsStaff ThisStaff { get; set; }
    }
 
}