using BusinessLayer;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace Restaurant
{
    public class CookStaff : Employee
    {
        public OrderStatus orderStatus { get; set; }

        //timestamp for cooks receiving order from waitstaff
        public DateTime timeOrderReceived { get; set; }

        //timestamp for cooks completing order for delivery
        public DateTime timeOrderComplete { get; set; }

        //method for marking an order received
        public void ReceiveOrder()
        {
            timeOrderReceived = DateTime.Now;

        }

        //method for marking an order finished for delivery
        public void CompleteOrder()
        {
            timeOrderComplete = DateTime.Now;

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
            
            else if (orderStatus == OrderStatus.CooksReceived)
            {
                MessageBox.Show("The customer's order is currently being prepared.");
            }

            else if (orderStatus == OrderStatus.CooksComplete)
            {
                MessageBox.Show("The customer's order is finished and ready for deliver to the table.  The order was completed by the kitchen at: " + timeOrderComplete);
            }

            else
            {
                MessageBox.Show("The food is still being prepared. The order was received by the kitchen at: " + timeOrderReceived);
            }

        }

    }
}