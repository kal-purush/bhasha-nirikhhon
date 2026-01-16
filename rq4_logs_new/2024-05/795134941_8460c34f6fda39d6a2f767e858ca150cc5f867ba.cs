using System;
using System.Collections.Generic;

namespace ClassLibrary
{
    public class clsStockCollection
    {




        //private data member for the list
        List<clsStock> _StockList = new List<clsStock>();
        public List<clsStock> StockList
        {
            get
            {
                //return the private data
                return _StockList;
            }

            set
            {
                //return the private data
                _StockList = value;
            }
        }
        public int Count
        {
            get
            {
                return _StockList.Count;
            }

            set
            {

            }
        }
        public clsStock ThisStock { get; set; }


        public clsStockCollection()
        {
           //variable for the index
           Int32 Index = 0;
            //variable to store the record count 
            Int32 RecordCount = 0;
            //object for the data connect
            clsDataConnection DB = new clsDataConnection();
            //execute the stored procedure
            DB.Execute("sproc_tblStock_SelectAll");
            //get the count of the records
            RecordCount = DB.Count; 
            //while there are records to process
            while (Index < RecordCount)
            {
                //create a blank stock 
                clsStock aStock = new clsStock();
                aStock.stockId = Convert.ToInt32(DB.DataTable.Rows[Index]["StockId"]);
                aStock.BookId = Convert.ToInt32(DB.DataTable.Rows[Index]["BookId"]);
                aStock.Quantity = Convert.ToInt32(DB.DataTable.Rows[Index]["Quantity"]);
                aStock.DateAdded = Convert.ToDateTime(DB.DataTable.Rows[Index]["DateAdded"]);
                aStock.SupplierId = Convert.ToInt32(DB.DataTable.Rows[Index]["SupplierId"]);
                //add the records to the private data member 
                _StockList.Add(aStock);
                //point at the next record
                Index++;
            }
        }

        
    }
}