using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FilmDTOLibrary
{
    public class LocationDTO
    {
        private int _idFilm;
        private string _username;
        private DateTime _dateDebut;
        private DateTime _dateFin;


        public int IdFilm
        {
            get
            {
                return _idFilm;
            }

            set
            {
                _idFilm = value;
            }
        }

        public string Username
        {
            get { return _username; }
            set { _username = value; }
        }

        public DateTime DateDebut
        {
            get { return _dateDebut; }
            set { _dateDebut = value; }
        }

        public DateTime DateFin
        {
            get { return _dateFin; }
            set { _dateFin = value; }
        }

        public LocationDTO(int idFilm, string user, DateTime dated, DateTime datef)
        {
            IdFilm = idFilm;
            Username = user;
            DateDebut = dated;
            DateFin = datef;
        }

        public LocationDTO()
        {
        }
    }
}