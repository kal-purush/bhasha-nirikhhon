using System;
using System.ComponentModel.DataAnnotations;

namespace NeutrackAPI.DTOs
{
    public class ProductUpdateDTO
    {

        [Required(ErrorMessage = "Name is required")]
        public string Name { get; set; }

        [Required(ErrorMessage = "Calories per gram is required")]
        public double CaloriesPerGram { get; set; }

        public double ProteinInGrams { get; set; }
        public double FatInGrams { get; set; }
        public double CarbInGrams { get; set; }

    }
}