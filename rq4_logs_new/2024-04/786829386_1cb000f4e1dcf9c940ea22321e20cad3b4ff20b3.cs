namespace UIS
{
    internal class Program
    {
        struct TStudent
        {
            public string jmeno;
            public string prijmeni;
            public string obor;
            public int rokNarozeni;
        }

        static int NactiCislo(string text = "Zadejte cislo: ", int pocetPokusu = 3, int altHodnota = 0)
        {
            bool povedlose = true;
            int cislo;
            do
            {
                if (povedlose == false)
                {
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine("Chybne zadani, pocet pokusu {0}\a", --pocetPokusu);
                    Console.ResetColor();
                }
                Console.Write(text);
                povedlose = int.TryParse(
                    Console.ReadLine(),

                    out cislo);

            } while (povedlose == false && pocetPokusu != 1);
            if (pocetPokusu == 1)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Pokusy vycerpany,automaticky nastavena hodnota {0}.", altHodnota);
                cislo = altHodnota;
                Console.ResetColor();
                Thread.Sleep(3000);
            }
            return cislo;
        }
        static TStudent Pridat()
        {
            TStudent student;
            Console.Write("Zadejte Jmeno: ");
            student.jmeno = Console.ReadLine();
            Console.Write("Zadejte Prijmeni: ");
            student.prijmeni = Console.ReadLine();
            Console.Write("Zadejte Obor: ");
            student.obor = Console.ReadLine();
            student.rokNarozeni = NactiCislo("Zadejte Rok narozeni: ", 4, 1990);



            return student;
        }
        static void vypisStudenta(TStudent[] studenti, int i)
        {
            Console.WriteLine("{0}\t {1}\t {2}\t {3}", studenti[i].jmeno, studenti[i].prijmeni, studenti[i].obor, studenti[i].rokNarozeni);
        }


        static TStudent[] Vymazat(TStudent[] studenti, int pocet, int i)
        {
            for (int y = i; y < pocet - 1; y++)
            {
                studenti[y] = studenti[y + 1];
            }
            return studenti;
        }

        static void Vymazat2(ref TStudent[] studenti, int pocet, int i)
        {
            for (int y = i; y < pocet - 1; y++)
            {
                studenti[y] = studenti[y + 1];
            }
        }
        static void Uloz(TStudent[] studenti, int pocet)
        {
            using (StreamWriter sw = new StreamWriter(@"soubor.txt"))
            {
                for (int i = 0; i < pocet; i++)
                {
                    sw.WriteLine("{0}, {1}, {2}, {3}", studenti[i].jmeno, studenti[i].prijmeni, studenti[i].obor, studenti[i].rokNarozeni);
                }
                sw.Flush();
            }
        }
        static int Nacti(ref TStudent[] studenti)
        {
            int pocet = 0;
            using (StreamReader sr = new StreamReader(@"soubor.txt"))
            {
                string radek;
                string[] pole = new string[4];
                while (!(sr.EndOfStream))
                {
                    radek = sr.ReadLine();
                    pole = radek.Split(',');
                    studenti[pocet].jmeno = pole[0];
                    studenti[pocet].prijmeni = pole[1];
                    studenti[pocet].obor = pole[2];
                    studenti[pocet].rokNarozeni = int.Parse(pole[3]);
                    pocet++;
                }
            }
            return pocet;
        }

        static void VypisDleJmena(
                                    TStudent[] studenti,
                                    int pocet,
                                    string jmeno)
        {
            for (int i = 0; i < pocet; i++)
            {
                if (jmeno == studenti[i].jmeno)
                {
                    vypisStudenta(studenti, i);
                }
            }
        }

        static void Main(string[] args)
        {
            TStudent[] studenti = new TStudent[20];
            int pocet = 0, i;
            string s;
            char odpoved;
            do
            {
                Console.Clear();
                Console.WriteLine("UIS v 1.0 - MENU");
                Console.WriteLine("------------------------");
                Console.WriteLine("Přidat studenta [p]");
                Console.WriteLine("Vypsat studenta [v]");
                Console.WriteLine("Vypsat studenty [w]");
                Console.WriteLine("Vypsat studenty dle jmena [j]");
                Console.WriteLine("Vymazat studenta [m]");
                Console.WriteLine("Ulozit do souboru [u]");
                Console.WriteLine("Nacist studenty [n]");
                Console.WriteLine("Ukončit program [k]");
                Console.Write("Zadejte akci: ");
                odpoved = char.ToLower(Console.ReadKey().KeyChar);
                switch (odpoved)
                {
                    case 'p':
                        Console.Clear();
                        Console.WriteLine("UIS v 1.0 - Pridani studenta");
                        Console.WriteLine("------------------------");
                        studenti[pocet] = Pridat();
                        pocet++;
                        break;
                    case 'v':
                        Console.Clear();
                        Console.WriteLine("UIS v 1.0 - Vypsani studenta");
                        Console.WriteLine("------------------------");
                        Console.Write("Zadejte index: ");
                        i = int.Parse(Console.ReadLine());
                        vypisStudenta(studenti, i);
                        Console.ReadKey();
                        break;
                    case 'w':
                        Console.Clear();
                        Console.WriteLine("UIS v 1.0 - Vypsani studentů");
                        Console.WriteLine("------------------------");
                        for (int y = 0; y < pocet; y++)
                        {
                            vypisStudenta(studenti, y);
                        }
                        Console.ReadKey();
                        break;
                    case 'j':
                        Console.Clear();
                        Console.WriteLine("UIS v 1.0 - Vypsani studentů dle jmena");
                        Console.WriteLine("------------------------");
                        Console.Write("Zadejte jmeno: ");
                        s = Console.ReadLine();
                        VypisDleJmena(studenti, pocet, s);
                        Console.ReadKey();
                        break;
                    case 'm':
                        Console.Clear();
                        Console.WriteLine("UIS v 1.0 - Vymazani studenta");
                        Console.WriteLine("------------------------");
                        Console.Write("Zadejte index: ");
                        i = int.Parse(Console.ReadLine());
                        studenti = Vymazat(studenti, pocet, i);
                        pocet--;
                        break;
                    case 'n':
                        Console.Clear();
                        Console.WriteLine("UIS v 1.0 - Nacteni studentu");
                        Console.WriteLine("------------------------");
                        pocet = Nacti(ref studenti);
                        Console.WriteLine("Nacteno");
                        Console.ReadKey();
                        break;
                    case 'u':
                        Console.Clear();
                        Console.WriteLine("UIS v 1.0 - Ukladani studentu");
                        Console.WriteLine("------------------------");
                        Uloz(studenti, pocet);
                        Console.WriteLine("Ulozeno");
                        Console.ReadKey();
                        break;
                    case 'k':
                        break;
                    default:
                        Console.WriteLine("\nSpatna volba!");
                        Console.ReadKey();
                        break;
                }
            } while (odpoved != 'k');
        }
    }
}