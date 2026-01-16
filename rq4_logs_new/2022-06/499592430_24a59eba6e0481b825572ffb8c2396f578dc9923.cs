using System.Windows;
using System.Data.SqlClient;
using System.Configuration;

namespace ClothingStore
{
    /// <summary>
    /// Lógica de interacción para Create.xaml
    /// </summary>
    public partial class Create : Window
    {
        SqlConnection sqlconn;
        public Create()
        {
            InitializeComponent();

            string conn = ConfigurationManager.ConnectionStrings["ClothingStore.Properties.Settings.ClothingStoreManagementConnectionString"].ConnectionString;

            sqlconn = new SqlConnection(conn); // Instantiate sql connection
        }

        private void bCancelCreate_Click(object sender, RoutedEventArgs e)
        {
            Close();
        }

        private void bAcceptCreate_Click(object sender, RoutedEventArgs e)
        {

            string quer = "INSERT INTO Customers (name,address,city,phone) VALUES (@nameCust, @addressCust, @cityCust, @phoneCust)";

            SqlCommand sqlCom = new SqlCommand(quer, sqlconn);

            sqlconn.Open();

            sqlCom.Parameters.AddWithValue("@nameCust", txtCreateName.Text);
            sqlCom.Parameters.AddWithValue("@addressCust", txtCreateAddress.Text);
            sqlCom.Parameters.AddWithValue("@cityCust", txtCreateCity.Text);
            sqlCom.Parameters.AddWithValue("@phoneCust", txtCreatePhone.Text);

            sqlCom.ExecuteNonQuery();

            sqlconn.Close();

            Close();
        }
    }
}