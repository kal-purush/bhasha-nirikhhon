using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace MortgageCalculator
{
	public class Customer
	{
		//declare varibales 
		private string _id;
		public required string firstName;
		public required string lastName;
		private string _email;
		private List<MortgageCalc> _customerMortgage;
		private MortgageCalc _createMortgage;

	

		//create get/set methods
		public string Id { get { return _id; } set { _id = value; } }
		public string FirstName { get; set; } = string.Empty;
		public string LastName { get; set; } = string.Empty;
		public string Email { get { return _email; } set { _email = value; } }
		public List<MortgageCalc> CustomerMortgage => _customerMortgage;


		//initialize with constructor
		//public void CustomerConstructor(string id, string firstName, string lastName, string email)
		//{
		//	_id = id;
		//	FirstName = firstName;
		//	LastName = lastName;
		//	Email = email;

		//}

		public Customer(string id, string firstName, string lastName, string email, MortgageCalc createMortgage)
		{
			Id = id;
			this.firstName = firstName;
			this.lastName = lastName;
			Email = email;
			//this._createMortgage = createMortgage;
			//this._createMortgage = new List<MortgageCalc>();
		}

		

		//public class CustomerConstructor
		//{
		//	public Customer Customer;
		//	public void CustomerConstr(string id, string firstName, string lastName, string email) 
		//	{
		//		Customer = new Customer() {
		//			_id = id,
		//			FirstName = firstName,
		//			LastName = lastName,
		//			Email = email,
		//		}
		//	}

		//}

		//write method to get loan information
		public List<string> LoanStatus(string id, string email)
		{
			//read from persistant file TO BE CREATED class to write to file
			//MortgageCalc mortgageCalc = new MortgageCalc();
			List<string> loanStatus = new List<string>();
			if (Id == id && Email == email)
			{
				loanStatus.Add("");
			}
			return loanStatus;
			

		}

		//write method to create loan
		public void CreateMortgage(decimal loanAmount, decimal interestRate, int loanDuration)
		{
			var createMortgage = new MortgageCalc(loanAmount, interestRate, loanDuration);
			
			_customerMortgage.Add(createMortgage);
		}



		//write method to remove loan after complete payment
	}
}