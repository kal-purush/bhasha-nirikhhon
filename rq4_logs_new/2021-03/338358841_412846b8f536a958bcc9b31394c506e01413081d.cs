using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;


namespace FlightClasses
{
    public class clsStaff
    {
        //private data membr for the Staff ID property
        private Int32 mStaffID;
        //private data member for the Email property
        private string mEmail;
        //private data member for the Name property
        private string mName;
        //private data member for the Password property
        private string mPassword;
       


        public bool Find(int StaffID)
        {
            //create an instance of the data connection
            clsDataConnection DB = new clsDataConnection();
            //add the parameter for the Staff ID to search for
            DB.AddParameter("@StaffID", StaffID);
            //execute the stored proceedure
            DB.Execute("sproc_tblStaff_FilterByStaffID");
            //if one record is found (there should be either one or zero!)
            if (DB.Count == 1)
            {
                mStaffID = Convert.ToInt32(DB.DataTable.Rows[0]["StaffID"]);
                mEmail = Convert.ToString(DB.DataTable.Rows[0]["Email"]);
                mName = Convert.ToString(DB.DataTable.Rows[0]["Name"]);
                mPassword = Convert.ToString(DB.DataTable.Rows[0]["Password"]);
                //return that everything worked ok
                return true;
            }
            //if no record was found 
            else
            {
                //return false indicating a problem
                return false;
            }
        }





        public string Valid(string someEmail, string someName, string somePassword)
        {
            //string variable to store the error message
            string Error = "";

            //if the  staff Email is more than 30 characters
            if (someEmail.Length > 30)
            {
                //return an error message
                Error = Error + "The staff email cannot have more than 30 characters";
            }
            if (someEmail.Length == 0 | someEmail.Length < 4)
            {
                //return an error message
                Error = Error + "The  Staff Email may not be blank";
            }

            //if the name of the staff is more than 20 characters
            if (someName.Length > 20)
            {
                //return an error message
                Error = Error + "The staff name cannot have more than 20 characters";
            }
            if (someName.Length == 0 | someName.Length < 1)
            {
                //return an error message
                Error = Error + "The staff name may not be blank";
            }
            

            //if the Password is more than 20 characters
            if (somePassword.Length > 20)
            {
                //return an error message
                Error = Error + "The Password cannot have more than 20 characters";
            }
            if (somePassword.Length == 0 | somePassword.Length < 6)
            {
                //return an error message
                Error = Error + "The Password may not be blank";
            }
            return Error;
        }


        //public property for the StaffID
        public int StaffID
        {
            get
            {
                //return the private data
                return mStaffID;
            }
            set
            {
                //set the value of the private data member
                mStaffID = value;
            }
        }

        //public property for the Email
        public string Email
        {
            get
            {
                //return the private data
                return mEmail;
            }
            set
            {
                //set the value of the private data member
                mEmail = value;
            }
        }

        //public property for the name
        public string Name
        {
            get
            {
                //return the private data
                return mName;
            }
            set
            {
                //set the value of the private data member
                mName = value;
            }
        }

        //public property for the password
        public string Password
        {
            get
            {
                //return the private data
                return mPassword;
            }
            set
            {
                //set the value of the private data member
                mPassword = value;
            }
        }
    }
}