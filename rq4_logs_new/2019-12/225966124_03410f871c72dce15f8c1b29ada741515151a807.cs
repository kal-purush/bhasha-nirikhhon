using System;
using System.Collections.Generic;
using System.Linq;
using System.ComponetModel.DataAnnotations;
using System.Threading.Tasks;

namespace PetAdoptSite.Models
{
    /// <summary>
    /// Represents a single pet
    /// </summary>
    public class Pets
    {
        [Reqired]
        [Key]
        public int Id { get; set; }

        /// <summary>
        /// The Pet commonly known Breed
        /// </summary>
        [Reqired]
        [StringLength(20)]
        public string Breed { get; set; }

        /// <summary>
        /// Disired Pet rating
        /// </summary>
        [Range(0,10)]
        public double Rating { get; set; }

        /// <summary>
        /// Details about the pet behaver and care
        /// </summary>
        public string Descripton { get; set; }

        /// <summary>
        /// The price of the pet
        /// </summary>
        [DataType(DataType.Currency)]
        public double Price { get; set; }

    }
}