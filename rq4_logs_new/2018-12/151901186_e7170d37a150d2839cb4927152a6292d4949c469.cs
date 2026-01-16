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
    /// Interaction logic for ProductAdd.xaml
    /// </summary>
    public partial class ProductAdd : Window
    {
        private Product loadedProduct = null;
        private DateTime date = new DateTime();

        public ProductAdd()
        {
            InitializeComponent();
            LblProductId.Visibility = Visibility.Hidden;
            TxtProductId.Visibility = Visibility.Hidden;
        }
        public ProductAdd(Product product)
        {
            InitializeComponent();
            loadedProduct = product;
            PopulateControlsWithProduct(product);
        }

        //load product in all controls
        private void PopulateControlsWithProduct(Product selectedProduct)
        {
            //Change Header to edit / populate controls with product / Save Btn
            LblHead.Content = "Edit Product";
            TxtProductId.Text = selectedProduct.ProductId.ToString();
            TxtProductName.Text = selectedProduct.ProductName.ToString();
            TxtProductPrice.Text = selectedProduct.ProductPrice.ToString();
            TxtProductTax.Text = selectedProduct.ProductTax.ToString();
            TxtProductQuantity.Text = selectedProduct.ProductQuantity.ToString();
            BtnCreateProduct.Content = "Save Product";
        }

        private void BtnCreateProduct_Click(object sender, RoutedEventArgs e)
        {
            bool isValid = false;

            //Creating a new product MUST HAVE ProductName.
            if (!string.IsNullOrEmpty(TxtProductName.Text))
            {
                if (!string.IsNullOrEmpty(TxtProductPrice.Text))
                {
                    try
                    {
                        decimal pprice = Convert.ToDecimal(TxtProductPrice.Text);
                        isValid = true;
                    }
                    catch
                    {
                        isValid = false;
                        MessageBox.Show("Product Price must be a valid price", "Numbers only");
                    }
                }
                if (!string.IsNullOrEmpty(TxtProductTax.Text))
                {
                    try
                    {
                        decimal ptax = Convert.ToDecimal(TxtProductTax.Text);
                        isValid = true;
                    }
                    catch (Exception ex)
                    {
                        isValid = false;
                        MessageBox.Show("Product Tax must be a valid tax price", "Decimal only");
                    }
                }
                if (!string.IsNullOrEmpty(TxtProductQuantity.Text))
                {
                    try
                    {
                        int pquantity = Convert.ToInt32(TxtProductQuantity.Text);
                        isValid = true;
                    }
                    catch (Exception ex)
                    {
                        isValid = false;
                        MessageBox.Show("Product Quantity must be an Integer", "Whole Number Only");
                    }
                }
            }
            else
                MessageBox.Show("You MUST enter a Product name", "Attention", MessageBoxButton.OK);

            if (isValid)
            {
                if(BtnCreateProduct.Content.Equals("Save Product"))
                {
                    try
                    {
                        string productName = TxtProductName.Text;
                        decimal prodPrice = Convert.ToDecimal(TxtProductPrice.Text);
                        decimal prodTax = Convert.ToDecimal(TxtProductTax.Text);
                        int prodQuan = Convert.ToInt32(TxtProductQuantity.Text);
                        date = loadedProduct.ProductListedDate;
                        int currentId = loadedProduct.ProductId;
                        loadedProduct = new Product(currentId, productName, prodPrice, prodTax, prodQuan, date);
                        ProductDB.UpdateProduct(loadedProduct);
                        Window WindowPList = new ProductsList();
                        WindowPList.Show();
                        Close();
                    }
                    catch(Exception ex) { MessageBox.Show(ex.Message.ToString()); }
                }
                else
                {
                    try
                    {
                        string productName = TxtProductName.Text;
                        decimal prodPrice = Convert.ToDecimal(TxtProductPrice.Text);
                        decimal prodTax = Convert.ToDecimal(TxtProductTax.Text);
                        int prodQuan = Convert.ToInt32(TxtProductQuantity.Text);
                        date = DateTime.Now;
                        loadedProduct = new Product(productName, prodPrice, prodTax, prodQuan, date);
                        //ProductDB.CreateProduct(loadedProduct);
                        ProductDB.CreateProductAdd(loadedProduct);
                        Window WindowProductList = new ProductsList();
                        WindowProductList.Show();
                        Close();
                    }
                    catch(Exception ex) { MessageBox.Show(ex.Message.ToString()); }
                }
            }
        }

        private void BtnCancelProduct_Click(object sender, RoutedEventArgs e)
        {
            Window WindowCancelProductAdd = new ProductsList();
            WindowCancelProductAdd.Show();
            Close();
        }
    }
}