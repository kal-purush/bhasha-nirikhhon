using System;
using System.Collections.Generic;

namespace FlightClasses
{
    public class clsOrderlineCollection
    {
        //private data member for the list
        List<clsOrderline> mOrderlineList = new List<clsOrderline>();
        //private data member ThisOrderline
        clsOrderline mThisOrderline = new clsOrderline();


        //public property for ThisOrderline
        public clsOrderline ThisOrderline
        {
            get
            {
                //return the private data
                return mThisOrderline;
            }
            set
            {
                //set the private data
                mThisOrderline = value;
            }
        }



        //public property for the Orderline List
        public List<clsOrderline> OrderlineList
        {
            get
            {
                //return the private data
                return mOrderlineList;
            }
            set
            {
                //set the private data
                mOrderlineList = value;
            }
        }

        //public property for count
        public int Count
        {
            get
            {
                //return the count of the list
                return mOrderlineList.Count;
            }
            set
            {
                //we shall worry about this later
            }
        }

        //constructor for the class
        public clsOrderlineCollection()
        {
            //object for data connection
            clsDataConnection DB = new clsDataConnection();
            //execute the stored proceedure
            DB.Execute("sproc_tblOrderline_SelectAll");
            //populate the array list with the data table
            PopulateArray(DB);
        }

        public int Add()
        {
            //adds a new record to the database based on the values of mThisOrderline
            //connect to the database
            clsDataConnection DB = new clsDataConnection();
            //set the paramaters for the stored proceedure
            DB.AddParameter("@BookingNo", mThisOrderline.BookingNo);
            DB.AddParameter("@Quantity", mThisOrderline.Quantity);
            DB.AddParameter("@BookingID", mThisOrderline.BookingID);
            //execute the query returning the primary key value
            return DB.Execute("sproc_tblOrderline_Insert");
        }

        public void Delete()
        {
            //deletes the record pointed to by ThisOrderline
            //connect to the database
            clsDataConnection DB = new clsDataConnection();
            //set the parameter for the stored proceedure
            DB.AddParameter("@OrderlineID", mThisOrderline.OrderlineID);
            //execute the stored proceedure
            DB.Execute("sproc_tblOrderline_Delete");
        }

        public void Update()
        {
            //update the existing record based on the values of thisOrderline
            //connect to the database
            clsDataConnection DB = new clsDataConnection();
            //set the parameters for the stored proceedure
            DB.AddParameter("@OrderlineID", mThisOrderline.OrderlineID);
            DB.AddParameter("@BookingNo", mThisOrderline.BookingNo);
            DB.AddParameter("@Quantity", mThisOrderline.Quantity);
            DB.AddParameter("@BookingID", mThisOrderline.BookingID);
            //execute the stored proceedure
            DB.Execute("sproc_tblOrderline_Update");

        }

        public void ReportByBookingNo(string BookingNo)
        {
            //filters the records based on a full or partial BookingNo
            //connect to the database
            clsDataConnection DB = new clsDataConnection();
            //send the bookingNo parameter to the database
            DB.AddParameter("@BookingNo", BookingNo);
            //execute the stored proceedure
            DB.Execute("sproc_tblOrderline_FilterByBookingNo");
            //populate the array list with the data table
            PopulateArray(DB);
        }


        void PopulateArray(clsDataConnection DB)
        {
            //populate the array list based on the data table in the parameter DB
            //var for the index
            Int32 Index = 0;
            //var to store the record count
            Int32 RecordCount;
            //get the count of records
            RecordCount = DB.Count;
            //clear the private array list
            mOrderlineList = new List<clsOrderline>();
            //while there are records to process
            while (Index < RecordCount)
            {
                //create a blank orderline
                clsOrderline AOrderline = new clsOrderline();
                //read in the fields from the current record
                AOrderline.OrderlineID = Convert.ToInt32(DB.DataTable.Rows[Index]["OrderlineID"]);
                AOrderline.BookingNo = Convert.ToString(DB.DataTable.Rows[Index]["BookingNo"]);
                AOrderline.Quantity = Convert.ToInt32(DB.DataTable.Rows[Index]["Quantity"]);
                AOrderline.BookingID = Convert.ToInt32(DB.DataTable.Rows[Index]["BookingID"]);
                //add the record to the private data member
                mOrderlineList.Add(AOrderline);
                //point at the next record
                Index++;

            }
        }
    }
}