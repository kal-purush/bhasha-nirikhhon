using BusinessLayer;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace Restaurant
{
    class WaitStaff : Employee
    {
        public OrderStatus orderStatus { get; set; }
        public TableStatus tableStatus { get; set; }

        public DateTime timeOrderEntered { get; set; }

        public void TakeCustomerOrder()
        {
               
            
            //Enter Customer's Order

            //Time order was entered could also be saved
            timeOrderEntered = DateTime.Now;
            

            //Output information to Cooks

        }

        public void CheckOrderStatus()
        {
            //Boolean check to see if the Customer's order has been delivered
            //If not delivered, see how long order has been preparing...?
            //Would need OrderStatus to be set once Cook class has prepared food

            if (orderStatus == OrderStatus.Delivered)
            { 
                MessageBox.Show("The customer's order has been delivered.");
            }
            else
            {
                MessageBox.Show("The food is still being prepared. The order was entered at: " + timeOrderEntered);
            }

        }

        public void CashOutCustomer()
        {
            int totalBill = 0;
            bool customerTransactionComplete = false;
            //Cash out Customer's total bill



            //Trigger an event to set TableStatus to TableStatus.Dirty
            if (customerTransactionComplete == true)
            {
                tableStatus = TableStatus.Dirty;
            }
            
        }
    }
}