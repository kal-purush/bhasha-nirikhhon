namespace BudgetTourist.Housing
{
    using Contracts;

    public class Hostel : Housing, IHostel
    {
        public Hostel(string name, int numberOfPeople, string location, decimal price, string parkingSpace, int numberOfBeds)
            : base(name, location, price, parkingSpace)
        {
            this.Name = name;
            this.Location = location;
            this.Price = price;
            this.ParkingSpace = parkingSpace;
            this.NumberOfBeds = numberOfBeds;
        }

        public int NumberOfBeds { get; private set; }
    }
}