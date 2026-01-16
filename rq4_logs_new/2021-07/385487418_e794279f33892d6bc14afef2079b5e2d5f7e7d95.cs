namespace BankAccount
{
    class Account
    {
        private string _name;
        private double _money;

        public Account(string v1, double v2)
        {
            
        }

        public double withdrawal(double i)
        {
            return i;
        }

        public void deposit(double i)
        {
            
        }

        public double balance()
        {
            return _money;
        }

        public override string ToString()
        {
            return $"{_name}: {_money}";
        }

        public string Name
        {
            get => _name;
            set => _name = value;
        }
    }
}