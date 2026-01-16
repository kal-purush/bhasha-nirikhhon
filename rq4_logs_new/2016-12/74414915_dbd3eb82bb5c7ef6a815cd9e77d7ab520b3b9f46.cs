using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Shapes;
using System.Globalization;

namespace BizWorks
{
    /// <summary>
    /// Interaction logic for TransactionAdd.xaml
    /// </summary>
    public partial class TransactionAdd: Window
    {
        private string sessionUserTransactionAdd;
        private string transactionAddUsernamePass;

        public TransactionAdd(string sessionUserMainMenu)
        {
            InitializeComponent();
            sessionUserEmployeeAdd1 = sessionUserMainMenu;     
        }

        public void TextBox_GotFocus(object sender, RoutedEventArgs e)
        {
            TextBox tb = (TextBox)sender;
            tb.Text = string.Empty;
            tb.GotFocus -= TextBox_GotFocus;
        }


        private void Next_Click(object sender, RoutedEventArgs e)
        {
            int assetID, customerID;
            bool isCredit;
            float transactionAmount;
            DateTime transactionDate;
            string category, notes;
            
            cust = customerID.Text;
            iscredit = isCredit.Text;
            amount = TransactionAmount.Text;
            cgory = Category.Text;
            transactDate = DateTime.today();
            assID = assetID.Text;
            createdOn = DateTime.today();
            createdBy = Username.Text;
            notes = NotesTextBox.Text;

            //adds on to the back
            //so access with TransactionCollection.last();
            TransactionCollection.AddTransaction(iscredit, cust, amount,  cgory,
            transactDate, assID, DateTime createdOn, createdBy, notes);

            App.Current.MainWindow = main;
            this.Close();
            main.Show();
        }
    }
}