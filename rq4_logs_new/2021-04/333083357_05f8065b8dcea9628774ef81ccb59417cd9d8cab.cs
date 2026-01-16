using System;
using System.Collections.Generic;

namespace ClassLibrary
{
    public class clsOrderCollection
    {
        private List<clsOrders> mOrdersList = new List<clsOrders>();
        private clsOrders mThisOrder = new clsOrders();
        public clsOrderCollection()
        {
            Int32 Index = 0;
            Int32 RecordCount = 0;
            Int32 SearchOrderId = 0;
            clsDataConnection DB = new clsDataConnection();
            DB.Execute("sproc_tblOrder__SelectAll");
            RecordCount = DB.Count;

            while (Index < RecordCount)
            {
                clsOrders Orders = new clsOrders();

                Orders.OrderId = Convert.ToInt32(DB.DataTable.Rows[Index]["OrderId"]);
                SearchOrderId = Convert.ToInt32(DB.DataTable.Rows[Index]["OrderId"]);
                Orders.DispatchedStatus = Convert.ToBoolean(DB.DataTable.Rows[Index]["DispatchedStatus"]);
                Orders.DeliveryAddress = Convert.ToString(DB.DataTable.Rows[Index]["DeliveryAddress"]);
                Orders.OrderDate = Convert.ToDateTime(DB.DataTable.Rows[Index]["OrderDate"]);

                clsDataConnection DBO = new clsDataConnection();
                DBO.AddParameter("@OrderId", SearchOrderId);
                DBO.Execute("sproc_tblOrderline_Filter_By_OrderId");
                Orders.ItemId = Convert.ToInt32(DBO.DataTable.Rows[0]["ItemId"]);
                Orders.Quantity = Convert.ToInt32(DBO.DataTable.Rows[0]["Quantity"]);
                Orders.ProductCode = Convert.ToString(DBO.DataTable.Rows[0]["ProductCode"]);
                Orders.UnitPrice = Convert.ToDouble(DBO.DataTable.Rows[0]["UnitPrice"]);

                mOrdersList.Add(Orders);

                Index++;
            }
        }
        public List<clsOrders> OrdersList
        {
            get
            {
                return mOrdersList;
            }
            set
            {
                mOrdersList = value;
            }
        }
        public int Count
        {
            get
            {
                return mOrdersList.Count;
            }
            set
            {
                // TODO
            }
        }
        public clsOrders ThisOrder
        {
            get
            {
                return mThisOrder;
            }
            set
            {
                mThisOrder = value;
            }
        }

        public int Add()
        {
            clsDataConnection DB = new clsDataConnection();
            DB.AddParameter("@OrderId", mThisOrder.OrderId);
            DB.AddParameter("@OrderDate", mThisOrder.OrderDate);
            DB.AddParameter("@DistpatchedStatus", mThisOrder.DispatchedStatus);
            DB.AddParameter("@DeliveryAddress", mThisOrder.DeliveryAddress);

            DB.Execute("sproc_tblOrder_Insert");

            clsDataConnection DBO = new clsDataConnection();
            DBO.AddParameter("@OrderId", mThisOrder.OrderId);
            DBO.AddParameter("@ItemId", mThisOrder.ItemId);
            DBO.AddParameter("@ProductCode", mThisOrder.ProductCode);
            DBO.AddParameter("@Quantity", mThisOrder.Quantity);
            DBO.AddParameter("@UnitPrice", mThisOrder.UnitPrice);

            return DBO.Execute("sproc_tblOrderline_Insert");
        }
    }
}