using System.Collections.Generic;

namespace DecompNumber.App.DTO.DTO
{
    public class DecompositionDTO
    {
        public List<int> DividingNumbers { get; set; }
        public List<int> PrimeNumbers { get; set; }
        public string Error { get; set; }

        public DecompositionDTO()
        {
            DividingNumbers = new List<int>();
            PrimeNumbers = new List<int>();
        }
    }
}