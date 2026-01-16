using DAL;
using FilmDTOLibrary;
using System;
using System.Collections.Generic;
using System.Data.Linq;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BLL
{
    public partial class BLLClass
    {
        DalDataContext Dal;
        
        
        public BLLClass()
        {
            Dal = new DalDataContext();

        }

        public Actor GetActorbyID(int i) 
        {
            List <Actor> ListAct = new List<Actor>();
            var query = from a in Dal.Actors where a.id == i select a;
            return query.FirstOrDefault<Actor>();

        }
    }
}