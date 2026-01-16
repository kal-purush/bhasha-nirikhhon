package app;

public class SalesEmployee extends Employee {
	
	private float _comissionRate;
	private float _totalSales;
	
	// Constructors
		public SalesEmployee(int newEmployeeNumber, String newName, String newAddress, String newNIN, String newBankNumber, 
				float newStartingSalary, float comissionRate, float totalSales) {
			super(newName, newAddress, newNIN, newBankNumber, newStartingSalary);
			super.setEmployeeNumber(newEmployeeNumber);
			this.set_comissionRate(comissionRate);
			this.set_totalSales(totalSales);
		}

		public float get_comissionRate() {
			return _comissionRate;
		}

		public void set_comissionRate(float _comissionRate) {
			this._comissionRate = _comissionRate;
		}

		public float get_totalSales() {
			return _totalSales;
		}

		public void set_totalSales(float _totalSales) {
			this._totalSales = _totalSales;
		}
		
		

}