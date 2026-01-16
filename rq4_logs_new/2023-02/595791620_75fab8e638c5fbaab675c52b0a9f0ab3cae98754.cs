using System;
using static Program;

public class Program
{
    public class Automobil
    { 
        public string Naziv { get; private set; }
        public int GodinaProizvodnje { get; private set; }
        public double OsnovnaCijena { get; private set; }
        public Automobil(string naziv, int godinaProizvodnje, double osnovnaCijena)
        {
            this.Naziv = naziv;
            this.GodinaProizvodnje = godinaProizvodnje;
            this.OsnovnaCijena = osnovnaCijena;
        }

        public int Starost()
        {
            int result = 2023 - GodinaProizvodnje;
            return result;
        }

        public double UkupnaCijena()
        {
            double result1 = 0;

            if (OsnovnaCijena <= 70000)
                result1 = OsnovnaCijena * 1.3;
            else if (OsnovnaCijena > 70000 && OsnovnaCijena < 100000)
                result1 = OsnovnaCijena * 1.4;
            else
                result1 = OsnovnaCijena * 1.5;

            return result1;
        }

        public Action<Automobil, int> NaPromjenuGodineProizvodnje;
        public int GodProiz
        {
            get { return GodinaProizvodnje; }
            set
            {
                if (value == GodinaProizvodnje)
                {

                }
                else
                {
                    GodinaProizvodnje = value;
                    this?.NaPromjenuGodineProizvodnje(this, GodinaProizvodnje);
                }
            }
        }
    }
    public static void Main()
    {
        Console.Write("Unesite ime automobila: ");
        string nazivAuta = Console.ReadLine();

        Console.Write("Unesite godinu proizvodnje automobila: ");
        int godProiz = Convert.ToInt32(Console.ReadLine());

        Console.Write("Unesite cijenu automobila: ");
        double cijenaAuta = Convert.ToDouble(Console.ReadLine());

        var a = new Automobil(nazivAuta, godProiz, cijenaAuta);
        a.NaPromjenuGodineProizvodnje += (oriGod, novaGod) => Console.WriteLine("Starost automobila je: " + a.Starost() + '\n');

        a.GodProiz = 2015;

        //Console.Write("Unesite novu godinu proizvodnje automobila: ");
        //int novaGodProizvodnje = Convert.ToInt32(Console.ReadLine());
        //a.GodProiz = novaGodProizvodnje;

        Console.WriteLine(a.Naziv + "\t");
        Console.WriteLine(a.GodinaProizvodnje + "\t");
        Console.WriteLine(a.OsnovnaCijena + "\t");
    }
}