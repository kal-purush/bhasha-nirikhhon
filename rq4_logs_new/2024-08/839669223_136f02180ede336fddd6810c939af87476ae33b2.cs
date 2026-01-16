using CSharpFunctionalExtensions;
using FindAnimal.Domain.Shared;

namespace FindAnimal.Domain.SpeciesAggregate.Entities
{
    public class Breed : Shared.Entity<BreedId>
    {
        private Breed(BreedId id) : base(id) { }
        private Breed(BreedId id, string name) : base(id)
        {
            Name = name;
        }

        public string Name { get; private set; }


        public static Result<Breed> Create(BreedId id, string name)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                return Result.Failure<Breed>($"Invalid name {nameof(name)} cannot be null or empty");
            }

            var breedValue = new Breed(id, name);
            return breedValue;
        }
    }
}