using System;

namespace ClassLibrary
{
    public class clsOrder
    {
        private Int32 mOrderId;
        private Int32 mCustomerId;
        private Int32 mBookId;
        private DateTime mOrderDate;
        private Int32 mOrderPrice;
        public String mOrderStatus;
        public string OrderStatus
        {
            get
            {
                return mOrderStatus;
            }
            set
            {
                mOrderStatus = value;
            }
        }
        public int CustomerId
        {
            get
            {
                return mCustomerId;
            }
            set
            {
                mCustomerId = value;
            }
        }
        public int BookId
        {
            get
            {
                return mBookId;
            }
            set
            {
                mBookId = value;
            }
        }
        public DateTime OrderDate
        {
            get
            {
                return mOrderDate;
            }
            set
            {
                mOrderDate = value;
            }
        }
        public int OrderPrice
        {
            get
            {
                return mOrderPrice;
            }
            set
            {
                mOrderPrice = value;
            }
        }
        public Int32 OrderId
        {
            get
            {
                return mOrderId;
            }
            set
            {
                mOrderId = value;
            }
        }

        public bool Find(int orderId)
        {
            clsDataConnection DB = new clsDataConnection();
            DB.AddParameter("@OrderId", orderId);
            DB.Execute("sproc_OrderTable_FilterByOrderId");
            if (DB.Count == 1)
            {
                mOrderId = Convert.ToInt32(DB.DataTable.Rows[0]["Order_ID"]);
                mCustomerId = Convert.ToInt32(DB.DataTable.Rows[0]["Customer_ID"]);
                mBookId = Convert.ToInt32(DB.DataTable.Rows[0]["Book_ID"]);
                mOrderDate = Convert.ToDateTime(DB.DataTable.Rows[0]["Order_Date"]);
                mOrderPrice = Convert.ToInt32(DB.DataTable.Rows[0]["Order_Price"]);
                mOrderStatus = Convert.ToString(DB.DataTable.Rows[0]["Order_Status"]);
                return true;
            }
            else
            {
                return false;

            }

        }

        public string Valid(string orderPrice, string orderStatus, string orderDate)
        {


            String Error = "";
            int orderprice;
            DateTime DateTemp;




            if (orderPrice.Length == 0)
            {
                Error = Error + " Order Price cannot be blank";
            }

            orderprice = Convert.ToInt32(orderPrice);
            if (orderprice >= 2147483647)
            {
                Error = Error + "the value of Order price reached on the limit";
            }
            if (orderprice < 0)
            {
                Error = Error + "The value of Order Price cannot be negative";
            }




            if (orderStatus.Length == 0)
            {
                Error = Error + "Order STatus may not be blank";
            }
            if (orderStatus.Length > 50)
            {
                Error = Error + "Order Status has been reached maximum char";
            }







            DateTemp = Convert.ToDateTime(orderDate);
            if (DateTemp < DateTime.Now.Date)
            {
                Error = Error + "The date cannot be in the past";
            }
            if (DateTemp > DateTime.Now.Date)
            {
                Error = Error + "The Date cannot be in the future";
            }







            return Error;
        }
    }
}