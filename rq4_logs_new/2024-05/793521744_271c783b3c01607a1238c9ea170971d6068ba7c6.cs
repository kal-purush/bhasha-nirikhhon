using System;
using System.Collections.Generic;

namespace ClassLibrary
{
    public class clsOrdersCollection
    {
        //private data member for the list
        List<clsOrders> mOrdersList = new List<clsOrders>();
        //public property for the adddress list
        public List<clsOrders> OrdersList
        {
            get
            {
                //return the private data
                return mOrdersList;
            }

            set
            {
                //set the private data 
                mOrdersList = value;
            }
        }
        public int Count {
            get
            {
                //return the private data
                return mOrdersList.Count;
            }

            set
            {
                //deal with this later loool 
            }
        }
        public clsOrders ThisOrder { get; set; }

        //constructor for the class
        public clsOrdersCollection()
        {
            //variable for the index
            Int32 Index = 0;
            //variable to store the record count 
            Int32 RecordCount = 0;
            //object for the data connect
            clsDataConnection DB = new clsDataConnection();
            //execute the stored procedure 
            DB.Execute("sproc_tblOrders_SelectAll");
            //get the count of the records
            RecordCount = DB.Count;
            //while there are records to process
            while (Index < RecordCount)
            {
                //create a blank order
                clsOrders AnOrder = new clsOrders();
                //read in the fields for the current record
                AnOrder.Complete = Convert.ToBoolean(DB.DataTable.Rows[Index]["Complete"]);
                AnOrder.OrderId = Convert.ToInt32(DB.DataTable.Rows[Index]["OrderId"]);
                AnOrder.CustomerId = Convert.ToInt32(DB.DataTable.Rows[Index]["CustomerId"]);
                AnOrder.TotalAmount = Convert.ToDecimal(DB.DataTable.Rows[Index]["TotalAmount"]);
                AnOrder.OrderDate = Convert.ToDateTime(DB.DataTable.Rows[Index]["OrderDate"]);
                AnOrder.Status = Convert.ToString(DB.DataTable.Rows[Index]["Status"]);
                //add the record to the private data member
                mOrdersList.Add( AnOrder );
                //point at the next record 
                Index++;
            }
        }
    }
}