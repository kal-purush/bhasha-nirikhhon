using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace ExpressiveData.Sample.Models.Repositories
{
	class CustomerRepository : SampleDataRepository
	{
		public Customer GetCustomer(int id)
		{
			var sql = "SELECT * FROM Customers WHERE Id = @id";
			var parameters = new[]
			{
				new SqlParameter("@id", id)
			};

			Customer customer = null;
			Action<IExpressiveReader<Customer>> callback = r =>
			{
				r.Model = customer = new Customer();

				r.Read(m => m.Id);
				r.Read(m => m.Name);
				r.Read(m => m.Address);
				r.Read(m => m.City);
				r.Read(m => m.State);
				r.Read(m => m.ZipCode);
			};

			ExecuteQuery(sql, parameters, callback);

			return customer;
		}

		public IEnumerable<Customer> GetAllCustomers()
		{
			var sql = "SELECT * FROM Customers";

			var customers = new List<Customer>();
			Action<IExpressiveReader<Customer>> callback = r =>
			{
				r.Model = new Customer();

				r.Read(m => m.Id);
				r.Read(m => m.Name);
				r.Read(m => m.Address);
				r.Read(m => m.City);
				r.Read(m => m.State);
				r.Read(m => m.ZipCode);

				customers.Add(r.Model);
			};

			ExecuteQuery(sql, null, callback);

			return customers;
		}

		public async Task<IEnumerable<Customer>>  GetAllCustomersAsync()
		{
			var sql = "SELECT * FROM Customers";

			Func<IExpressiveReaderAsync<Customer>, Task<Customer>> callback = async r =>
			{
				r.Model = new Customer();

				await r.ReadAsync(m => m.Id);
				await r.ReadAsync(m => m.Name);
				await r.ReadAsync(m => m.Address);
				await r.ReadAsync(m => m.City);
				await r.ReadAsync(m => m.State);
				await r.ReadAsync(m => m.ZipCode);

				return r.Model;
			};

			return await ExecuteQueryAsync(sql, null, callback);
		}
	}
}