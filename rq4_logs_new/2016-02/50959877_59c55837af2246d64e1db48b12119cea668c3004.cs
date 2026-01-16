using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;

using PicnicOrm.Dapper.Demo;
using PicnicOrm.Dapper.Demo.Models;

namespace PicnicOrm.FakeDataGenerator
{
    class Program
    {
        static void Main(string[] args) { GenerateData(); }

        private static void GenerateData()
        {
            const int COUNT = 5000;
            Random rand = new Random();

            List<User> users = new List<User>();
            List<Car> cars = new List<Car>();
            List<Employer> employers = new List<Employer>();
            List<Address> addresses = new List<Address>();

            for (int i = 1; i <= COUNT; i++)
            {
                var user = new User() { Id = i, Name = Faker.NameFaker.Name(), BirthDate = Faker.DateTimeFaker.BirthDay() };
                var employer = new Employer() { Id = i, Name = Faker.CompanyFaker.Name(), EmployeeCount = Faker.NumberFaker.Number(10, 150000), Sector = Faker.EnumFaker.SelectFrom<Sector>() };
                var address = new Address()
                {
                    Id = i,
                    Street = Faker.LocationFaker.Street(),
                    City = Faker.LocationFaker.City(),
                    State = Faker.LocationFaker.Country(),
                    PostalCode = Faker.LocationFaker.PostCode()
                };
                var car = new Car() { Id = i, MakeModel = Faker.EnumFaker.SelectFrom<MakeModel>(), Year = Faker.NumberFaker.Number(1990, 2016) };

                users.Add(user);
                cars.Add(car);
                employers.Add(employer);
                addresses.Add(address);
            }

            foreach (var employer in employers)
            {
                employer.Address = addresses[rand.Next(COUNT)];
            }

            foreach (var user in users)
            {
                for (int i = 0; i <= 5; i++)
                {
                    var index = rand.Next(COUNT);
                    if (!user.Cars.Contains(cars[index]))
                    {
                        user.Cars.Add(cars[index]);
                    }
                }
                user.Address = addresses[rand.Next(COUNT)];
                user.Employer = employers[rand.Next(COUNT)];
            }

            Save(users, cars, employers, addresses);
        }

        private static void Save(IEnumerable<User> users, IEnumerable<Car> cars, IEnumerable<Employer> employers, IEnumerable<Address> addresses)
        {
            using (var connection = new SqlConnection(@"Server=(localdb)\ProjectsV12;Database=DemoDatabase;Integrated security=True"))
            {
                try
                {
                    connection.Open();

                    foreach (var car in cars)
                    {
                        SaveCar(car, connection);
                    }

                    foreach (var address in addresses)
                    {
                        SaveAddress(address, connection);
                    }

                    foreach (var employer in employers)
                    {
                        SaveEmployer(employer, connection);
                    }

                    foreach (var user in users)
                    {
                        SaveUser(user, connection);
                    }
                }
                finally
                {
                    connection.Close();
                }
            }
        }

        private static void SaveCar(Car car, SqlConnection connection)
        {
            var cmd = new SqlCommand($"INSERT INTO dbo.Car VALUES ({car.Id}, {(int)(car.MakeModel)}, {car.Year})", connection);
            cmd.ExecuteNonQuery();
        }

        private static void SaveAddress(Address address, SqlConnection connection)
        {
            var cmd = new SqlCommand($"INSERT INTO dbo.Address VALUES ({address.Id}, '{Sanitize(address.Street)}', '{Sanitize(address.City)}', '{Sanitize(address.State)}', '{Sanitize(address.PostalCode)}')", connection);
            cmd.ExecuteNonQuery();
        }

        private static void SaveEmployer(Employer employer, SqlConnection connection)
        {
            var cmd = new SqlCommand($"INSERT INTO dbo.Employer VALUES ({employer.Id}, '{Sanitize(employer.Name)}', {employer.EmployeeCount}, {(int)(employer.Sector)}, {employer.Address.Id})", connection);
            cmd.ExecuteNonQuery();
        }

        private static void SaveUser(User user, SqlConnection connection)
        {
            var cmd = new SqlCommand($"INSERT INTO dbo.[User] VALUES ({user.Id}, '{Sanitize(user.Name)}', '{Sanitize(user.BirthDate.ToShortDateString())}', {user.Address.Id}, {user.Employer.Id})", connection);
            cmd.ExecuteNonQuery();

            foreach (var car in user.Cars)
            {
                cmd = new SqlCommand($"INSERT INTO dbo.UserCar VALUES({user.Id}, {car.Id})", connection);
                cmd.ExecuteNonQuery();
            }
        }

        private static string Sanitize(string parameter) => parameter.Replace("'", " ");
    }
}