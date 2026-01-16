using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;

namespace WebApp.DAL
{
    public class Pet
    {
        public string BeaconID { get; set; }

        public string Name { get; set; }

        public PetKind Kind { get; set; }

        public PetStatus Status { get; set;}

        public string Description { get; set; }
    }

    public enum PetKind
    {
        Dog = 1,
        Cat = 2,
        Other = 100
    }

    public enum PetStatus
    {

        Owned = 1,
        Lost = 2
    }
}