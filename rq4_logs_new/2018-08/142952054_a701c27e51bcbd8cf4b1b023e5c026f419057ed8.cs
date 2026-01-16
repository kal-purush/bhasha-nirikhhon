namespace Ch6_P2_Employees
{
    public class Intern : Employee
    {
        public Intern()
        {
        }

        public Intern(string name, int id, float pay) : base(name, id, pay)
        {
        }

        public Intern(string name, int age, int id, float pay) : base(name, age, id, pay)
        {
        }

        public Intern(string name, int age, int id, float pay, string ssn) : base(name, age, id, pay, ssn)
        {
        }

        public override void DisplayStats()
        {
            base.DisplayStats();
        }

        public override bool Equals(object obj)
        {
            return base.Equals(obj);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        public override void GiveBonus(float amount)
        {
            base.GiveBonus(amount);
        }

        public override string ToString()
        {
            return base.ToString();
        }
    }
}