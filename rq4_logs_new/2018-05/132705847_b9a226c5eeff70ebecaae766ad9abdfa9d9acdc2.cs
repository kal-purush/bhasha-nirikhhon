using CarParking.Interfaces;

namespace CarParking.Classes
{
	class Car : ICar
	{
		public Car(int id, double balance, CarType type)
		{
			CarId = id;
			CarBalance = balance;
			CarType = type;
		}

		#region ICar Members

		public int CarId { get; private set; }

		public double CarBalance { get; private set; }

		public CarType CarType { get; private set; }

		public void PayRent(double rent)
		{
			CarBalance -= rent;
		}

		public void AddToBalance(double money)
		{
			CarBalance += money;
		}

		#endregion
	}
}