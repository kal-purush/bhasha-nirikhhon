
using System.Windows;
using System.Data.SqlClient;
using System.Configuration;

namespace ClothingStore
{
    /// <summary>
    /// Lógica de interacción para UpdateWindow.xaml
    /// </summary>
    public partial class UpdateWindow : Window
    {
        SqlConnection sqlconn;

        private int z;
        public UpdateWindow(int id)
        {
            InitializeComponent();

            z = id;

            string conn = ConfigurationManager.ConnectionStrings["ClothingStore.Properties.Settings.ClothingStoreManagementConnectionString"].ConnectionString;

            sqlconn = new SqlConnection(conn); // Instantiate sql connection
        }

        private void bCancelUpdate_Click(object sender, RoutedEventArgs e)
        {
            Close();
        }

        private void bAcceptUpdate_Click(object sender, RoutedEventArgs e)
        {

            string quer = "UPDATE Customers SET name=@nameCust WHERE Id="+z;

            SqlCommand sqlCom = new SqlCommand(quer, sqlconn);

            sqlconn.Open();

            sqlCom.Parameters.AddWithValue("@nameCust", txtUpdateName.Text);

            sqlCom.ExecuteNonQuery();

            sqlconn.Close();

            Close();



        }
    }
}