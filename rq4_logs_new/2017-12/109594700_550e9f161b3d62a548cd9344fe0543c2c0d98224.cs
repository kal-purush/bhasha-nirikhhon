using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace GPIO_RGM
{
    public class GPIO
    {
        private static int[] colIDs = { 1, 2, 3 };

        private static GPIOController.Pin[] vendPins = {
                GPIOController.Pin.GPIO14,       // 1
                GPIOController.Pin.GPIO15,       // 2
                GPIOController.Pin.GPIO18        // 3
            };

        private static GPIOController.Pin[] emptyPins = {
                GPIOController.Pin.GPIO25,   // 1
                GPIOController.Pin.GPIO8,    // 2
                GPIOController.Pin.GPIO7     // 3
            };

        private static double[] moneyIDs = { 0.25, 1.00 };   // $0.25, $1.00
        private static GPIOController.Pin[] moneyPins = {
                GPIOController.Pin.GPIO10,
                GPIOController.Pin.GPIO9,
            };
        private static GPIOController.Pin returnPin = GPIOController.Pin.GPIO11;
        private static WriteMotors motorControl = new WriteMotors(vendPins, colIDs);
        private static ReadEmpty emptyMonitor = new ReadEmpty(emptyPins, colIDs);
        private static ReadMoney moneyMonitor = new ReadMoney(moneyPins, moneyIDs, returnPin);

        // Testing!
        public static void dispenseItem(int itemID)
        {
            try
            {
                motorControl.SetLow(itemID);
                //emptyMonitor.ReadOnce();
                System.Threading.Thread.Sleep(300); // Sleep for 3 
                motorControl.SetHigh(itemID);
                //emptyMonitor.ReadOnce();
            }
            catch (Exception e)
            {
                MessageBox.Show(e.Message, "MACHINE ERROR", MessageBoxButtons.OK, MessageBoxIcon.Error);
                return;
            }
            
        }


    }
}