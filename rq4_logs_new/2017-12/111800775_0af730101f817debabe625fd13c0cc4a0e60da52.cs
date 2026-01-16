using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SmartVideoDBDTO
{
    public class HitsDTO
    {
        private int _idHits;
        private DateTime _date;
        private string _type;
        private int _nbRecherche;
        private int _idRequete;

        public int IdHits { get => _idHits; set => _idHits = value; }
        public DateTime Date { get => _date; set => _date = value; }
        public string Type { get => _type; set => _type = value; }
        public int NbRecherche { get => _nbRecherche; set => _nbRecherche = value; }
        public int IdRequete { get => _idRequete; set => _idRequete = value; }
        public HitsDTO()
        {

        }
        public HitsDTO(int idHits,DateTime date, string type, int nbRecheche, int idRequete)
        {
            IdHits = idHits;
            Date = date;
            Type = type;
            NbRecherche = nbRecheche;
            IdRequete = idRequete;
        }

    }
}