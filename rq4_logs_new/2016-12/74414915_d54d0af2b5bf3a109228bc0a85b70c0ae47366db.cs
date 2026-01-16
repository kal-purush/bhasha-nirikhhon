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
        private string passedCustomer;
        private string passedAsset;

        public TransactionAdd(string sessionUserMainMenu, string customerName, string assetName)
        {
            InitializeComponent();
            sessionUserTransactionAdd = sessionUserMainMenu;
            passedCustomer = customerName;
            passedAsset = assetName;

            CustomerID.Content = passedCustomer;
            AssetName.Content = passedAsset;
        }

        public void TextBox_GotFocus(object sender, RoutedEventArgs e)
        {
            TextBox tb = (TextBox)sender;
            tb.Text = string.Empty;
            tb.GotFocus -= TextBox_GotFocus;
        }


        private void Next_Click(object sender, RoutedEventArgs e)
        {
            string createdBy, notes;
            string lastUpdatedBy;
            DateTime createdOn, lastUpdatedOn;
            double transactAmount;
            string cgory;
            bool isCredit = true;

            if (IsCredit.Text == "Yes")
            {
                isCredit = true;
            }
            else if (IsCredit.Text == "No")
            {
                isCredit = true;
            }
            transactAmount = Convert.ToDouble(TransactionAmount.Text);
            cgory = TransactionCategory.Text;


            
            createdBy = sessionUserTransactionAdd;
            createdOn = DateTime.Now;
            lastUpdatedBy = sessionUserTransactionAdd;
            lastUpdatedOn = DateTime.Now;
            notes = NotesTextBox.Text;

            TransactionCollection.AddTransaction(isCredit, passedCustomer, transactAmount, cgory, createdOn,
                passedAsset, createdOn, createdBy, lastUpdatedOn, lastUpdatedBy, notes);

            MainMenu main = new MainMenu(sessionUserTransactionAdd);
            App.Current.MainWindow = main;
            this.Close();
            main.Show();
        }
    }
}