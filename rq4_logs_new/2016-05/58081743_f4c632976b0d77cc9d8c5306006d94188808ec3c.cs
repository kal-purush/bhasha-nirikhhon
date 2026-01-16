using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Bangazon
{
    class Bangazon
    {
        public double total { get; set; } 
        public int customerId { get; set; }
        public int paymentId { get; set; }
        public int customerOrderId { get; set; }
        public List<string> PendingOrder = new List<string>();

        public string line { get; set; }

        public void MainMenu()
        {
            Console.WriteLine("*********************************************************");
            Console.WriteLine("**  Welcome to Bangazon! Command Line Ordering System  **");
            Console.WriteLine("*********************************************************");
            Console.WriteLine("1.Create an account");
            Console.WriteLine("2.Create a payment option");
            Console.WriteLine("3.Order a product");
            Console.WriteLine("4.Complete an order");
            Console.WriteLine("5.See product popularity");
            Console.WriteLine("6.Leave Bangazon!");
        }

        public void CreateAccount()
        {
            Console.Clear();
            Console.WriteLine("Enter customer first name");
            var FirstName = Console.ReadLine();
            Console.WriteLine("Enter customer last name");
            var LastName = Console.ReadLine();
            Console.WriteLine("Enter street address");
            var Address = Console.ReadLine();
            Console.WriteLine("Enter city");
            var City = Console.ReadLine();
            Console.WriteLine("Enter state");
            var State = Console.ReadLine();
            Console.WriteLine("Enter postal code");
            var Zip = Console.ReadLine();
            Console.WriteLine("Enter phone number");
            var Phone = Console.ReadLine();


            string command = "INSERT INTO Customer(FirstName, LastName, StreetAddress, City, State, PostalCode, PhoneNumber) VALUES('" + FirstName + "', '" + LastName + "', '" + Address + "', '" + City + "', '" + State + "', '" + Zip + "', '" + Phone + "')";

            SqlConnection sqlConnection1 =
                new SqlConnection("Data Source=(LocalDB)\\MSSQLLocalDB;database=Invoice;AttachDbFilename=\"C:\\Users\\Tyler\\Documents\\Visual Studio 2015\\Projects\\Bangazon\\Bangazon\\Invoice.mdf\";Integrated Security=True");
            

            SqlCommand cmd = new SqlCommand();
            cmd.CommandType = System.Data.CommandType.Text;
            cmd.CommandText = command;
            cmd.Connection = sqlConnection1;

            sqlConnection1.Open();
            cmd.ExecuteNonQuery();
            sqlConnection1.Close();

            Console.Clear();

            this.MainMenu();
        }

        public void SelectCustomer()
        {
            Console.Clear();
            Console.WriteLine("Which customer?");
            string query = @"
SELECT 
  IdCustomer,
  FirstName + ' ' + LastName
FROM Customer c
";

            using (SqlConnection connection = new SqlConnection("Data Source=(LocalDB)\\MSSQLLocalDB;database=Invoice;AttachDbFilename=\"C:\\Users\\Tyler\\Documents\\Visual Studio 2015\\Projects\\Bangazon\\Bangazon\\Invoice.mdf\";Integrated Security=True"))
            using (SqlCommand cmd = new SqlCommand(query, connection))
            {
                connection.Open();
                using (SqlDataReader reader = cmd.ExecuteReader())
                {
                    // Check is the reader has any rows at all before starting to read.
                    if (reader.HasRows)
                    {
                        // Read advances to the next row.
                        while (reader.Read())
                        {
                            Console.WriteLine("{0}, {1}",
                                reader[0], reader[1]);
                        }

                    }
                }
            }
            this.CreatePaymentOption();

        }

        public void CreatePaymentOption()
        {
            string CustomerId = Console.ReadLine();

            Console.WriteLine("Enter payment type and press return.");
            var PaymentType = Console.ReadLine();
            Console.WriteLine("Enter account number and press return.");
            var AccountNumber = Console.ReadLine();

            string command = "INSERT INTO PaymentOption (Name, AccountNumber, IdCustomer) VALUES('" + PaymentType + "', '" + AccountNumber + "', '" + CustomerId + "')";

            SqlConnection sqlConnection1 =
                new SqlConnection("Data Source=(LocalDB)\\MSSQLLocalDB;database=Invoice;AttachDbFilename=\"C:\\Users\\Tyler\\Documents\\Visual Studio 2015\\Projects\\Bangazon\\Bangazon\\Invoice.mdf\";Integrated Security=True");


            SqlCommand cmd = new SqlCommand();
            cmd.CommandType = System.Data.CommandType.Text;
            cmd.CommandText = command;
            cmd.Connection = sqlConnection1;

            sqlConnection1.Open();
            cmd.ExecuteNonQuery();
            sqlConnection1.Close();

            Console.Clear();
            this.MainMenu();

        }

        public void LoadProducts()
        {
            Console.Clear();
            Console.WriteLine("Choose a product to add to your order:");
            string query = @"
SELECT
  IdProduct, 
  Name,
  Price
FROM Product
";

            using (SqlConnection connection = new SqlConnection("Data Source=(LocalDB)\\MSSQLLocalDB;database=Invoice;AttachDbFilename=\"C:\\Users\\Tyler\\Documents\\Visual Studio 2015\\Projects\\Bangazon\\Bangazon\\Invoice.mdf\";Integrated Security=True"))
            using (SqlCommand cmd = new SqlCommand(query, connection))
            {
                connection.Open();
                using (SqlDataReader reader = cmd.ExecuteReader())
                {
                    // Check is the reader has any rows at all before starting to read.
                    if (reader.HasRows)
                    {
                        // Read advances to the next row.
                        while (reader.Read())
                        {
                            Console.WriteLine("{0}) {1}, ${2}",
                                reader[0], reader[1], reader[2]);
                        }

                    }
                }
            }
            Console.WriteLine("0) Return to Main Menu");

            this.SelectProduct();
        }

        public void SelectProduct()
        {
            Console.WriteLine("Enter the number of the desired product and press return to add it to your order");
            string ProductId = Console.ReadLine();
            int IdNumber;
            bool isNumber = Int32.TryParse(ProductId, out IdNumber);
            if (ProductId == "0")
            {
                Console.Clear();
                this.MainMenu();
            } else if (isNumber)
            {
                this.PendingOrder.Add(ProductId);
                Console.Clear();
                Console.WriteLine("Item added successfully! Press any key to go back to shopping.");
                Console.ReadKey();
                this.LoadProducts();
                
            }
            else
            {
                Console.Clear();
                Console.WriteLine("Product ID not found, press any key to try again...");
                Console.ReadKey();
                this.LoadProducts();
            }
           
        }

        public void CheckOrderStatus()
        {
            Console.Clear();
            if (PendingOrder.Count == 0)
            {
                Console.WriteLine("You have no items in your cart! Press any key to return to the main menu...");
                Console.ReadKey();
                Console.Clear();

                this.MainMenu();
            } else if (PendingOrder.Count > 0)
            {
                this.GetTotal();
            }
        }

        public void GetTotal()
        {

            this.PendingOrder.ForEach(orderId => 
            {
                string query = "SELECT Price FROM Product WHERE IdProduct = "+ Int32.Parse(orderId);

                using (SqlConnection connection = new SqlConnection("Data Source=(LocalDB)\\MSSQLLocalDB;database=Invoice;AttachDbFilename=\"C:\\Users\\Tyler\\Documents\\Visual Studio 2015\\Projects\\Bangazon\\Bangazon\\Invoice.mdf\";Integrated Security=True"))
                using (SqlCommand cmd = new SqlCommand(query, connection))
                {
                    connection.Open();
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        // Check is the reader has any rows at all before starting to read.
                        if (reader.HasRows)
                        {
                            // Read advances to the next row.
                            while (reader.Read())
                            {
                                this.total += (float)reader[0];
                            }

                        }
                    }
                }
            });

            Console.WriteLine("Your total is ${0}, proceed to checkout? (y/n)", Math.Round(this.total, 2));
            string response = Console.ReadLine();
            if (response == "n" || response == "N")
            {
                Console.Clear();
                this.MainMenu();
            } else if (response == "y" || response == "Y")
            {
                Console.Clear();
                this.GenerateOrderInfo();
            }
            
        }

        public void GenerateOrderInfo()
        {
            Console.WriteLine("Which customer is placing the order?");
            string query = @"
SELECT 
  IdCustomer,
  FirstName + ' ' + LastName
FROM Customer c
";

            using (SqlConnection connection = new SqlConnection("Data Source=(LocalDB)\\MSSQLLocalDB;database=Invoice;AttachDbFilename=\"C:\\Users\\Tyler\\Documents\\Visual Studio 2015\\Projects\\Bangazon\\Bangazon\\Invoice.mdf\";Integrated Security=True"))
            using (SqlCommand cmd = new SqlCommand(query, connection))
            {
                connection.Open();
                using (SqlDataReader reader = cmd.ExecuteReader())
                {
                    // Check is the reader has any rows at all before starting to read.
                    if (reader.HasRows)
                    {
                        // Read advances to the next row.
                        while (reader.Read())
                        {
                            Console.WriteLine("{0}, {1}",
                                reader[0], reader[1]);
                        }

                    }
                }
            }
            this.customerId = Int32.Parse(Console.ReadLine());
            Console.WriteLine("Choose a payment option:");
            string query2 = "SELECT IdPaymentOption, Name FROM PaymentOption WHERE IdCustomer =" + this.customerId;

            using (SqlConnection connection = new SqlConnection("Data Source=(LocalDB)\\MSSQLLocalDB;database=Invoice;AttachDbFilename=\"C:\\Users\\Tyler\\Documents\\Visual Studio 2015\\Projects\\Bangazon\\Bangazon\\Invoice.mdf\";Integrated Security=True"))
            using (SqlCommand cmd = new SqlCommand(query2, connection))
            {
                connection.Open();
                using (SqlDataReader reader = cmd.ExecuteReader())
                {
                    // Check is the reader has any rows at all before starting to read.
                    if (reader.HasRows)
                    {
                        // Read advances to the next row.
                        while (reader.Read())
                        {
                            Console.WriteLine("{0}, {1}",
                                reader[0], reader[1]);
                        }

                    }
                }
            }
            this.paymentId = Int32.Parse(Console.ReadLine());
            Console.Clear();
            this.CreateCustomerOrder();
        }

        public void CreateCustomerOrder()
        {
            string orderTime = DateTime.Now.ToString(@"MM\/dd\/yyyy h\:mm tt");
            string command = "INSERT INTO CustomerOrder(DateCreated, Shipping, IdCustomer, IdPaymentOption) VALUES('" + orderTime+ "', 'USPS', '" + this.customerId + "', '" + this.paymentId + "')";

            SqlConnection sqlConnection1 =
                new SqlConnection("Data Source=(LocalDB)\\MSSQLLocalDB;database=Invoice;AttachDbFilename=\"C:\\Users\\Tyler\\Documents\\Visual Studio 2015\\Projects\\Bangazon\\Bangazon\\Invoice.mdf\";Integrated Security=True");


            SqlCommand cmd = new SqlCommand();
            cmd.CommandType = System.Data.CommandType.Text;
            cmd.CommandText = command;
            cmd.Connection = sqlConnection1;

            sqlConnection1.Open();
            cmd.ExecuteNonQuery();
            sqlConnection1.Close();

            this.GetCustomerOrderId();
        }

        public void GetCustomerOrderId()
        {
            string query = "SELECT MAX(IdCustomerOrder) FROM CustomerOrder";

            using (SqlConnection connection = new SqlConnection("Data Source=(LocalDB)\\MSSQLLocalDB;database=Invoice;AttachDbFilename=\"C:\\Users\\Tyler\\Documents\\Visual Studio 2015\\Projects\\Bangazon\\Bangazon\\Invoice.mdf\";Integrated Security=True"))
            using (SqlCommand cmd = new SqlCommand(query, connection))
            {
                connection.Open();
                using (SqlDataReader reader = cmd.ExecuteReader())
                {
                    // Check is the reader has any rows at all before starting to read.
                    if (reader.HasRows)
                    {
                        // Read advances to the next row.
                        while (reader.Read())
                        {

                            this.customerOrderId = (int)reader[0];
                        }

                    }
                }
            }
            this.CreateOrderProducts();
        }

        public void CreateOrderProducts()
        {
            this.PendingOrder.ForEach(productId =>
            {
                string command = "INSERT INTO OrderProducts (IdProduct, IdCustomerOrder) VALUES('" + Int32.Parse(productId) + "', '" + this.customerOrderId + "')";

                SqlConnection sqlConnection1 =
                    new SqlConnection("Data Source=(LocalDB)\\MSSQLLocalDB;database=Invoice;AttachDbFilename=\"C:\\Users\\Tyler\\Documents\\Visual Studio 2015\\Projects\\Bangazon\\Bangazon\\Invoice.mdf\";Integrated Security=True");


                SqlCommand cmd = new SqlCommand();
                cmd.CommandType = System.Data.CommandType.Text;
                cmd.CommandText = command;
                cmd.Connection = sqlConnection1;

                sqlConnection1.Open();
                cmd.ExecuteNonQuery();
                sqlConnection1.Close();
            });

            this.total = 0;
            this.PendingOrder.Clear();
            Console.WriteLine("Your order is complete! Press any key to return to main menu.");
            Console.ReadKey();
            Console.Clear();
            this.MainMenu();
        }

        public void SortItemsByPopularity()
        {
            Console.Clear();
            string query = @"SELECT
p.Name,
COUNT(op.IdProduct) AS TimesOrdered, 
COUNT(DISTINCT co.IdCustomer) AS Customers ,
ROUND(SUM(p.Price), 2) AS total
FROM Product p 
INNER JOIN OrderProducts op 
  ON p.IdProduct = op.IdProduct 
INNER JOIN CustomerOrder co
  ON op.IdCustomerOrder = co.IdCustomerOrder 
GROUP BY p.Name 
ORDER BY TimesOrdered DESC";

            using (SqlConnection connection = new SqlConnection("Data Source=(LocalDB)\\MSSQLLocalDB;database=Invoice;AttachDbFilename=\"C:\\Users\\Tyler\\Documents\\Visual Studio 2015\\Projects\\Bangazon\\Bangazon\\Invoice.mdf\";Integrated Security=True"))
            using (SqlCommand cmd = new SqlCommand(query, connection))
            {
                connection.Open();
                using (SqlDataReader reader = cmd.ExecuteReader())
                {
                    // Check is the reader has any rows at all before starting to read.
                    if (reader.HasRows)
                    {
                        // Read advances to the next row.
                        while (reader.Read())
                        {
                            Console.WriteLine("{0} ordered {1} times by {2} customers for a total revenue of ${3}",
                                reader[0], reader[1], reader[2], reader[3]);
                        }

                    }
                }
                Console.WriteLine("");
                Console.WriteLine("Press any key to return to main menu.");
                Console.ReadKey();
                Console.Clear();
                this.MainMenu();
            }
        }
    }
}