using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace Web2ProjekatBackend.Models
{
    public class Oprema
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string OpremaType { get; set; }
        public string Coordinates { get; set; }
        public string Address { get; set; }

        public virtual ICollection<Incident> Incidenti { get; set; }

        public Oprema(int id, string name, string opremaType, string coordinates, string address)
        {
            Id = id;
            Name = name;
            OpremaType = opremaType;
            Coordinates = coordinates;
            Address = address;
        }
    }
}