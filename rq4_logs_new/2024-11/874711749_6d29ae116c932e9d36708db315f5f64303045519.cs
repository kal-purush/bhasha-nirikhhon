using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TheatreBO
{
    public class RepresentationVue
    {

        public Representation Representation { get; set; }

        public int Id { get; set; }

        public int Heure { get; set; }

        public DateTime Date { get; set; }

        public string Lieu { get; set; }

        public int NbPlaceMax { get; set; }

        public int IdTheatre { get; set; }

        public int Tarif { get; set; }

        public RepresentationVue(Representation representation)
        {
            Id = representation.id;
            Heure = representation.heure;
            Date = representation.date;
            Lieu = representation.lieu;
            NbPlaceMax = representation.nbPlaceMax;
            IdTheatre = representation.theatre.id;
            Tarif = representation.tarif;
        }
    }
}