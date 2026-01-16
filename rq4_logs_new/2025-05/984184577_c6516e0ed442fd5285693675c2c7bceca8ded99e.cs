namespace AnimalShelter.DataAccess.Entities
{
    public class AdoptionApplicationEntity
    {
        public Guid Id { get; set; }

        public DateOnly ApplicationDate { get; set; }

        public string Comment { get; set; } = string.Empty;

        //Внешние ключи
        public Guid UserId { get; set; }

        public Guid AnimalId { get; set; }

        public Guid StatusAdoptionId { get; set; }

        // Навигационные свойства
        public virtual UserEntity User { get; set; }
        public virtual AnimalEntity Animal { get; set; }
        public virtual StatusAdoptionEntity StatusAdoption { get; set; }
    }
}