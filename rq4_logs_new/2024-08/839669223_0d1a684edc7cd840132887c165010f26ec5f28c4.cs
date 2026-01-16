namespace FindAnimal.Domain
{
    public class Pet
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public AnimalType Type { get; set; }
        public string Description { get; set; }
        public string Breed { get; set; }
        public string Color { get; set; }
        public string HelthInformation { get; set; }
        public string LocationAddress { get; set; }
        public int Height { get; set; }
        public int Weigth { get; set; }
        public DateTime BirthDate { get; set; }
        public bool Castrated { get; set; }
        public bool Vaccinated { get; set; }
        public bool Visible { get; set; }
        public User Owner { get; set; }
        public Credentials Credentials { get; set; }
        public DateTime? creationDate { get; set; }

    }
}