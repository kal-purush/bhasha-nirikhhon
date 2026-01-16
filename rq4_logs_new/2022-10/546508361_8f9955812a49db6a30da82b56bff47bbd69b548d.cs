using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Domain
{
    public class Fridge
    {
        public Guid Id { get; set; }
        [Required]
        public string Name { get; set; }
        public string OwnerName { get; set; }
        [Required]
        public string ModelId { get; set; }
        public FridgeModel Model { get; set; }
        public ICollection<FridgeProducts> FridgeProducts { get; set; }
    }
}