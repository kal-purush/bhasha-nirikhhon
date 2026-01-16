using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HeskiAntonioZadatakZaKraj.Klase
{
    class Vozilo
    {
        string vrsta;
        double maxBrzina;
        int brojKotaca;

        public void setBrojKotaca(int brojKotaca)
        {
            this.brojKotaca = brojKotaca;
        }
        public int getBrojKotaca()
        {
            return this.brojKotaca;
        }

        public void setVrsta(string vrsta)
        {
            this.vrsta = vrsta;
        }
        public string getVrsta()
        {
            return this.vrsta;
        }

        public void setMaxBrzina(double maxBrzina)
        {
            this.maxBrzina = maxBrzina;
        }
        public double getMaxBrzina()
        {
            return this.maxBrzina;
        }


        public Vozilo(string vrsta, int maxBrzina, int brojKotaca)
        {
            this.vrsta = vrsta;
            this.maxBrzina = maxBrzina;
            this.brojKotaca = brojKotaca;
        }

        public Vozilo() { }

        public override string ToString()
        {
            return "Vrsta: " + vrsta + "\nMax brzina: " + maxBrzina + "\n Broj kotaca " + brojKotaca;
        }
    }
}
