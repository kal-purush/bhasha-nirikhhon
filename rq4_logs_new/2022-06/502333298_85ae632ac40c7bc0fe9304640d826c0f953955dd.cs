using System;
namespace HomeTestCrisp.Configurations
{
	public class OrderTransformationConfiguration
	{
		public int OrderNumber, Year, Month, Day;
		public string ProductNumber, ProductName, Count;

		public OrderTransformationConfiguration(string[] values)
		{
			this.OrderNumber = Convert.ToInt32(values[0]);
			this.Year = Convert.ToInt32(values[1]);
			this.Month = Convert.ToInt32(values[2]);
			this.Day = Convert.ToInt32(values[3]);
			this.ProductNumber = values[4];
			this.ProductName = values[5];
			this.Count = values[6];

		}
	}
}
