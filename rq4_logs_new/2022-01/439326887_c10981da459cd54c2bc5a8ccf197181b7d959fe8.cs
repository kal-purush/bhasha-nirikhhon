using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Windows;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Shapes;
using System.Configuration;
using System.Collections.ObjectModel;

namespace _1Semprojekt2022_Golf
{
    /// <summary>
    /// Interaction logic for Popup_tes_window.xaml
    /// </summary>
    public partial class Popup_tes_window : Window
    {
        //private List<Zipcode> list = new List<Zipcode>();
        public Popup_tes_window()
        {
            InitializeComponent();
            //Seek(0, "", "", 0, "", 0, "");
            /////*LoadGrid_Runner*/();
            this.Closing += new System.ComponentModel.CancelEventHandler(Create_runner_Closing);
        }


        void Create_runner_Closing(object sender, System.ComponentModel.CancelEventArgs e)
        {
            MainWindow secondWindow = new MainWindow();
            secondWindow.Show();
        }




        //private void Refresh()
        //{
        //    grid.ItemsSource = new ObservableCollection<Zipcode>(list);
        //}
        //private class Zipcode
        //{
        //    public int P_id { get; set; }
        //    public string P_name { get; set; }
        //    public string P_mail { get; set; }
        //    public int P_phone { get; set; }
        //    public string P_address { get; set; }
        //    public int P_zip { get; set; }
        //    public string P_city { get; set; }
        //}
        //private void grid_SelectionChanged(object sender, SelectionChangedEventArgs e)
        //{
        //    int n = grid.SelectedIndex;
        //    if (n >= 0)
        //    {
        //        Id.Text = list[n].P_id;
        //        Name.Text = list[n].P_name;
        //        Mail.Text = list[n].P_mail;
        //        Phone.Text = list[n].P_phone;
        //        Address.Text = list[n].P_address;
        //        Zip.Text = list[n].P_zip;
        //        City.Text = list[n].P_city;
        //    }
        //}
        //private void Clear()
        //{
        //    grid.SelectedIndex = -1;
        //    Name.Clear();
        //    Mail.Clear();
        //    Phone.Clear();
        //    Address.Clear();
        //    Zip.Clear();
        //    City.Clear();
        //    Seek(0, "", "", 0, "", 0, "");
        //}
        //private void Seek(int Id, string Name, string Mail, int Phone, string Address, int Zip, string City)
        //{
        //    int n = grid.SelectedIndex;
        //    string selected_id = list[n].P_id;

        //    SqlConnection connection = null;
        //    try
        //    {
        //        connection = new SqlConnection(ConfigurationManager.ConnectionStrings["post"].ConnectionString);
        //        SqlCommand command = new SqlCommand("SELECT * FROM Participant", connection);
        //        command.Parameters.Add(CreateParam("@P_id", Id + "%", SqlDbType.Int));
        //        command.Parameters.Add(CreateParam("@P_name", Name + "%", SqlDbType.NVarChar));
        //        command.Parameters.Add(CreateParam("@P_mail", Mail + "%", SqlDbType.NVarChar));
        //        command.Parameters.Add(CreateParam("@P_phone", Phone + "%", SqlDbType.Int));
        //        command.Parameters.Add(CreateParam("@P_address", Address + "%", SqlDbType.NVarChar));
        //        command.Parameters.Add(CreateParam("@P_zip", Zip + "%", SqlDbType.Int));
        //        command.Parameters.Add(CreateParam("@P_city", City + "%", SqlDbType.NVarChar));
        //        connection.Open();
        //        SqlDataReader reader = command.ExecuteReader();
        //        list.Clear();
        //        while (reader.Read()) list.Add(new Zipcode
        //        {
        //            P_id = (int)reader[0],
        //            P_name = reader[1].ToString(),
        //            P_mail = reader[2].ToString(),
        //            P_phone = (int)reader[3],
        //            P_address = reader[4].ToString(),
        //            P_zip = (int)reader[5],
        //            P_city = reader[6].ToString()
        //        });
        //        Refresh();
        //    }
        //    catch (Exception ex)
        //    {
        //        MessageBox.Show(ex.Message);
        //    }
        //    finally
        //    {
        //        if (connection != null) connection.Close();
        //    }
        //}
        //private SqlParameter CreateParam(string name, object value, SqlDbType type)
        //{
        //    SqlParameter param = new SqlParameter(name, type);
        //    param.Value = value;
        //    return param;
        //}
        //private void btnView_Click(object sender, RoutedEventArgs e)
        //{
        //    try
        //    {
        //        DataRowView dataRowView = (DataRowView)((Button)e.Source).DataContext;
        //    }
        //    catch (Exception ex)
        //    {
        //        //MessageBox.Show(ex.Message.ToString());
        //    }
        //    int n = grid.SelectedIndex;
        //    if (n >= 0)
        //    {
        //        Id.Text = list[n].P_id;
        //        Name.Text = list[n].P_name;
        //        Mail.Text = list[n].P_mail;
        //        Phone.Text = list[n].P_phone;
        //        Address.Text = list[n].P_address;
        //        Zip.Text = list[n].P_zip;
        //        City.Text = list[n].P_city;

        //        string Id_string = list[n].P_id;
        //        string Name_string = list[n].P_name;
        //        string Mail_string = list[n].P_mail;
        //        string Phone_string = list[n].P_phone;
        //        string Address_string = list[n].P_address;
        //        string Zip_string = list[n].P_zip;
        //        string City_string = list[n].P_city;

        //        Name.Clear();
        //        Mail.Clear();
        //        Phone.Clear();
        //        Address.Clear();
        //        Zip.Clear();
        //        City.Clear();
        //        Popup_test win2 = new Popup_test(Id_string, Name_string, Mail_string, Phone_string, Address_string, Zip_string, City_string);
        //        win2.Show();
        //    }
        //}

        //private void btnDelete_Click_2(object sender, RoutedEventArgs e)
        //{
        //    try
        //    {
        //        DataRowView dataRowView = (DataRowView)((Button)e.Source).DataContext;
        //    }
        //    catch (Exception ex)
        //    {
        //        //MessageBox.Show(ex.Message.ToString());
        //    }
        //    string error = "";
        //    int n = grid.SelectedIndex;
        //    string selected_id = list[n].P_id;
        //    SqlConnection connection = null;
        //    try
        //    {
        //        connection = new SqlConnection(ConfigurationManager.ConnectionStrings["post"].ConnectionString);
        //        SqlCommand command = new SqlCommand("Delete FROM Zipcodes WHERE Code = @Code", connection);
        //        command.Parameters.Add(CreateParam("@P_id", selected_id.Trim(), SqlDbType.NVarChar));


        //        //command.Parameters.Add(CreateParam("@P_name", Name + "%", SqlDbType.NVarChar));
        //        //command.Parameters.Add(CreateParam("@P_mail", Mail + "%", SqlDbType.NVarChar));
        //        //command.Parameters.Add(CreateParam("@P_phone", Phone + "%", SqlDbType.NVarChar));
        //        //command.Parameters.Add(CreateParam("@P_address", Address + "%", SqlDbType.NVarChar));
        //        //command.Parameters.Add(CreateParam("@P_zip", Zip + "%", SqlDbType.NVarChar));
        //        //command.Parameters.Add(CreateParam("@P_city", City + "%", SqlDbType.NVarChar));
        //        connection.Open();
        //        if (command.ExecuteNonQuery() == 1)
        //        {
        //            Clear();
        //            return;
        //        }
        //        error = "Illegal database operation";
        //    }
        //    catch (Exception ex)
        //    {
        //        error = ex.Message;
        //    }
        //    finally
        //    {
        //        if (connection != null) connection.Close();
        //    }
        //    MessageBox.Show(error);
        //}

    }
}