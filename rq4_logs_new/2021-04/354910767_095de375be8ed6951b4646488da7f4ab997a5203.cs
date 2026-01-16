using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Studio_Inventory_API.Models
{
    public class Equipment
    {
        public int Id { get; set; }
        public string SerialNumber { get; set; }
        public string Name { get; set; }
        public int CategoryId { get; set; }
        public virtual Category Category {get; set;}

        Equipment()
        {
        }

        Equipment(int id, string serialNumber, string name, int categoryId)
        {
            Id = id;
            SerialNumber = serialNumber;
            Name = name;
            CategoryId = categoryId;
        }
    }
}