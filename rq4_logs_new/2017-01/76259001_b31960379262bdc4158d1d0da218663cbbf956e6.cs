using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace HoneymoonShop.Models.DressFinderModels
{
    public class Review
    {
        public int ReviewID { get; set; }
        public String Naam { get; set; }
        public String Beoordeling { get; set; }
        public String klantReview { get; set; }

        //navigation Property
        public int JurkID { get; set; }
        public virtual Jurk Jurk { get; set; }



        public Review()
        {

        }
    }
}