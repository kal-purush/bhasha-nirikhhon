using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Footap
{
    class BeborereVm 
    {
        public ObservableCollection<Beborere> Beboreres { get; set; }
        public string Name { get; set; }
        public int Alder { get; set; }
        public int HusNr { get; set; }

        public BeborereVm()
        {
            Beboreres = new ObservableCollection<Beborere>();
            Beboreres.Add(new Beborere("Jens", 22, 74));
        }


        public void Add()
        {
            Beboreres.Add(new Beborere(Name, Alder, HusNr));
        }

        public void Remove()
        {
            foreach (Beborere beborere in Beboreres)
            {
                if (beborere.husNr == HusNr)
                {
                    Beboreres.Remove(beborere);
                    return;
                } 
            }
        }
    }
}