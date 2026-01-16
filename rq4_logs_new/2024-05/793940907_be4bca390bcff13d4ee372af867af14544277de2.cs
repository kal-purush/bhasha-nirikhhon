using System;

namespace ClassLibrary
{
    public class clsReceipt
    {
        public int ID { get; set;}
        public int OrderID { get; set; }
        public string Transation {  get; set; }
        public float Tax { get; set; }
        public string PaymentMethod { get; set; }
        public float TotalPrice {  get; set; }
        public DateTime CreatedAt {  get; set; }
        public string Valid(int ReceiptId, int OrderId, string Transaction,float Tax, string PaymentMethod, float TotalPrice, string CreatedAt)
        {
            // receipt validation
            if(ReceiptId<0)
            {
                return "Receipt cannot be a negative value";
            }
            else if(ReceiptId > 2147483647)
            {
                return "ReceiptID is out of boundary";
            }

            // order validation

            if (OrderId < 0)
            {
                return "order cannot be a negative value";
            }
            else if (OrderId > 2147483647)
            {
                return "order is out of boundary";
            }

            // transaction validation 

            if(Transaction.Length < 36)
            {
                return "transaction should not be less than 36";
            }
            else if(Transaction.Length > 36)
            {
                return "transaction should not be greater than 36";
            }

            // Tax validation
            if(Tax < 0)
            {
                return "the tax value should positive";
            }
            
            // PaymentMethod validation

            if(PaymentMethod == "")
            {
                return "payment Method cannot be empty";
            }
            else if(!(PaymentMethod.Length == 3|| PaymentMethod.Length == 12 || PaymentMethod.Length == 6))
            {
                return "payment Method name is unproper";
            }

            // TotalPrice validation

            if(TotalPrice < 0)
            {
                return "the TotalPrice value should positive";
            }

            // CreatedAt validation

            try
            {
                DateTime currentDate = DateTime.Now.Date;
                DateTime dateTime = Convert.ToDateTime(CreatedAt);
                if (dateTime < currentDate)
                {
                    return "The date cannot be in the past";
                }
                else if (dateTime > currentDate)
                {
                    return "The date cannot be in the future";
                }
            }
            catch
            {
                return "The date was not a valid date";
            }
            return "";
        }
    }
}