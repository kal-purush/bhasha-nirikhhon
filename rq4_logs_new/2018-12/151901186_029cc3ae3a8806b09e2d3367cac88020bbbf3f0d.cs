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
using ShoppingStore.DataAccess;
using ShoppingStore.DataBase;

namespace ShoppingStore
{
    /// <summary>
    /// Interaction logic for ReceiptList.xaml
    /// </summary>
    public partial class ReceiptList : Window
    {
        //public List<Receipt> Receipts => Extras.GetReceiptsList();
        public List<Receipt> receipts = null; 
        private Receipt receipt = null;
        private User user = null;
        private Customer customer = null;
        private int selectedIndex = -1;

        public ReceiptList()
        {
            InitializeComponent();
            PopulateReceiptListview();
        }
        public ReceiptList(Customer c)
        {
            InitializeComponent();
            PopulateReceiptsById(c.UserId);
            customer = c;
        }
        public ReceiptList(User u)
        {
            InitializeComponent();
            PopulateReceiptListview();
            user = u;
        }
        
        private void PopulateReceiptsById(int userid)
        {
            List<Receipt> plistreceipt = new List<Receipt>();
            ListViewReceipts.Items.Clear();
            try
            {
                receipts = Extras.GetReceiptsById(userid);
                plistreceipt = Extras.GetReceiptsById(userid);
                ListViewReceipts.ItemsSource = receipts;//plistreceipt;
            }
            catch(Exception ex) { MessageBox.Show(ex.Message.ToString()); }
        }
        private void PopulateReceiptListview()
        {
            List<Receipt> plistreceipt = new List<Receipt>();
            ListViewReceipts.Items.Clear();
            try
            {
                receipts = Extras.GetReceiptsList();
                plistreceipt = Extras.GetReceiptsList();
                ListViewReceipts.ItemsSource = receipts; //plistreceipt;
                //ListViewReceipts.ItemsSource = Receipts;
            } 
            catch(Exception ex) { MessageBox.Show(ex.Message.ToString()); }
        }

        private void BtnViewReceipt_Click(object sender, RoutedEventArgs e)
        {
            if (selectedIndex != -1)
            {
                if(receipt != null)
                {
                    string msg = "Are you sure you want to view this receipt: " + receipt.ReceiptID.ToString("00000");
                    MessageBoxResult boxResult = MessageBox.Show(msg, "Select Receipt", MessageBoxButton.YesNo, 
                                                                 MessageBoxImage.Question);
                    if(boxResult == MessageBoxResult.Yes)
                    {
                        if(customer != null)
                        {
                            Window WReceiptWindow = new ReceiptWindow(customer, receipt);
                            WReceiptWindow.Show();
                            Close();
                        }
                        else
                        {
                            Window WindowReceiptWindow = new ReceiptWindow(user, receipt);
                            WindowReceiptWindow.Show();
                            Close();
                        }
                    }
                }
            }
            else
                MessageBox.Show("No receipt selected", "Please Select");
        }

        private void ListViewReceipts_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            selectedIndex = ListViewReceipts.SelectedIndex;
            if (selectedIndex != -1)
            {
                //receipt = Receipts[selectedIndex];
                receipt = receipts[selectedIndex];
            }
        }

        private void BtnCancel_Click(object sender, RoutedEventArgs e)
        {
            if(user.IsAdmin == true)
            {
                Window WindowAdminMenu = new AdminWindow(user);
                WindowAdminMenu.Show();
                Close();
            }
            else if(user.IsCustomer == true)
            {
                Window WindowCustomerSrc = new CustomerScreen(user);
                WindowCustomerSrc.Show();
                Close();
            }
            else
            {
                Window WindowCustomerSrceen = new CustomerScreen();
                WindowCustomerSrceen.Show();
                Close();
            }
        }
    }
}