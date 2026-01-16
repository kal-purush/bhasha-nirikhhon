using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClassLibraryDuLepidoptere
{
    public class Chenille : StadeDEvolution
    {

        public Chenille() { }

        public override void SeDeplacer()
        {
            Console.WriteLine("Je rampe car je suis une chenille ;)");
        }

        public override StadeDEvolution SeMetamorphoser()
        {

            return new Chrysalide();
        }


    }
}