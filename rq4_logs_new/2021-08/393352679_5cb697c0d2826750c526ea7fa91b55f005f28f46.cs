

namespace BankSystem.Models
{
    public class Client : IPerson
    {
        public string Name { get; set; }
        public int Age { get; set; }
        public int Passport { get; set; }

        public override bool Equals(object obj)
        {
            if (obj == null)
            {
                return false;
            }
            if (!(obj is Client))
            {
                return false;
            }

            Client client = (Client)obj;
            return client.Name == Name &&
                client.Age == Age &&
                client.Passport == Passport;
        }
        public override int GetHashCode()
        {
            return Name.GetHashCode() + Age.GetHashCode() + Passport.GetHashCode();
        }
    }
}