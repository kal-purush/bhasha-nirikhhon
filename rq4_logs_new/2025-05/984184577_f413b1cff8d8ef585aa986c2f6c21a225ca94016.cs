namespace AnimalShelter.Core.Models
{
    public class Adoption
    {
        public const int MAX_NAME_LENGTH = 250;
        private Adoption(
            Guid id,
            DateOnly applicationDate,
            Guid userId,
            Guid animalId,
            Guid employeeId)
        {
            Id = id;
            ApplicationDate = applicationDate;
            UserId = userId;
            AnimalId = animalId;
            EmployeeId = employeeId;
        }

        public Guid Id { get; }

        public DateOnly ApplicationDate { get; }

        //Внешние ключи
        public Guid UserId { get; }

        public Guid AnimalId { get; }

        public Guid EmployeeId { get; }

        // Навигационные свойства
        public User User { get; private set; }

        public Animal Animal { get; private set; }

        public Employee Employee { get; private set; }

        public static (Adoption Adoption, string Error) Create(
            Guid id,
            DateOnly applicationDate,
            Guid userId,
            Guid animalId,
            Guid employeeId)
        {
            var error = string.Empty;

            // Проверка обязательных внешних ключей
            if (userId == Guid.Empty)
                error += string.IsNullOrEmpty(error) ? "User is required" : "\nUser is required";

            if (animalId == Guid.Empty)
                error += string.IsNullOrEmpty(error) ? "Animal is required" : "\nAnimal is required";

            if (employeeId == Guid.Empty)
                error += string.IsNullOrEmpty(error) ? "Employee info is required" : "\nEmployee info is required";

            var adoption = new Adoption(id, applicationDate, userId, animalId, employeeId);

            return (adoption, error);
        }
    }
}