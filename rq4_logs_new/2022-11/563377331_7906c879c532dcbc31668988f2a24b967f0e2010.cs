using System;
using System.Collections.Generic;
using System.Text;

namespace ProjectBank.Views
{
    class TransactionView
    {        
        public static void Run()
        {
            Console.Clear();

            string accountList = Session.User.GetAccList();

            string[] options = { "Exit" };
            ViewUI viewUI = new ViewUI("Make a transaction", options, accountList);

            

            int selectedOption = viewUI.Run();
            switch (selectedOption)
            {
                case 2:
                    DashboardView.Run();                    
                    break;
            }

        }
    }
}