using System;
using NutrientsApp.Entities.Abstract;

namespace NutrientsApp.Entities
{
    public class NutrientComponentEntity : IBaseEntity
    {
        public Guid Id { get; set; }
        public string Name { get; set; }
    }
}