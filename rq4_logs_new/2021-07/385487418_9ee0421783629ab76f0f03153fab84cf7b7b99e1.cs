namespace Firm
{
    public class Commission : Hourly
    {
        private double _totalSales = 0;
        private double _conversionRate = 0;

        public Commission(string eName, string eAddress, string ePhone, string socSecNumber, double rate, double ConversionRate) : base(eName,
            eAddress, ePhone, socSecNumber, rate) 
        {
            _conversionRate = ConversionRate;
        }

        public void AddSales(double totalSales)
        {
            _totalSales += totalSales;
        }

        public override double Pay()
        {
            var payment = payRate * _hoursWorked + _totalSales * _conversionRate;
            _totalSales = 0;
            return payment;
        }

        public override string ToString()
        {
            var result = base.ToString();
            result +="\nTotal sales: " + _totalSales;
            return result;
        }
    }
}