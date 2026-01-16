using System;
using System.Collections.Generic;

namespace ClassLibrary
{
    public class clsReceiptCollection
    {
        List<clsReceipt> mReceiptList = new List<clsReceipt>();
        clsReceipt mThisReceipt = new clsReceipt();
        public clsReceiptCollection()
        {
            int index = 0;
            int recordCount = 0;
            clsDataConnection DB = new clsDataConnection();
            DB.Execute("sproc_tblReceipt_SelectAll");
            recordCount = DB.Count;

            while (index < recordCount)
            {
                clsReceipt receipt = new clsReceipt();
                receipt.ID = Convert.ToInt32(DB.DataTable.Rows[index]["Receipt_ID"]);
                receipt.OrderID = Convert.ToInt32(DB.DataTable.Rows[index]["Order_ID"]);
                receipt.Transation = Convert.ToString(DB.DataTable.Rows[index]["Receipt_Transaction"]);
                receipt.Tax = (float)Convert.ToDouble(DB.DataTable.Rows[index]["Receipt_Tax"]);
                receipt.PaymentMethod = Convert.ToString(DB.DataTable.Rows[index]["Receipt_PaymentMethod"]);
                receipt.TotalPrice = (float)Convert.ToDouble(DB.DataTable.Rows[index]["Receipt_TotalPrice"]);
                receipt.CreatedAt = Convert.ToDateTime(DB.DataTable.Rows[index]["Receipt_createdAt"]);
                mReceiptList.Add(receipt);
                index++;
            }

        }
        
        public List<clsReceipt> ReceiptList 
        {
            get
            {
                return mReceiptList;
            }
            set 
            {
             mReceiptList = value; 
            } 
        }
        
        public int Count {
            get 
            { 
                return    mReceiptList.Count; 
            }
            set
            {

            }
       }
        public clsReceipt ThisReceipt { 
            get 
            {
                return mThisReceipt; 
            } 
            set 
            {
                mThisReceipt= value; 
            } 
        }
        public int Add()
        {
            clsDataConnection DB = new clsDataConnection();

            DB.AddParameter("@Tax", mThisReceipt.Tax);
            DB.AddParameter("@PaymentMethod", mThisReceipt.PaymentMethod);
            DB.AddParameter("@TotalPrice", mThisReceipt.TotalPrice);
            DB.AddParameter("@OrderID", mThisReceipt.OrderID);
            return DB.Execute("sproc_tblReceipt_Insert");
        }

        public void Update()
        {
            clsDataConnection DB = new clsDataConnection();
            DB.AddParameter("@ReceiptId",mThisReceipt.ID);
            DB.AddParameter("@OrderId",mThisReceipt.OrderID);
            DB.AddParameter("@Transaction",mThisReceipt.Transation);
            DB.AddParameter("@Tax", mThisReceipt.Tax);
            DB.AddParameter("@PaymentMethod", mThisReceipt.PaymentMethod);
            DB.AddParameter("@TotalPrice", mThisReceipt.TotalPrice);
            DB.AddParameter("@CreatedAt", mThisReceipt.CreatedAt);
            DB.Execute("sproc_tblReceipt_Update");
        }

        public void Delete()
        {
            clsDataConnection DB = new clsDataConnection();
            DB.AddParameter("@ReceiptId", mThisReceipt.ID);
            DB.Execute("sproc_tblReceipt_Delete");
        }
    }
}