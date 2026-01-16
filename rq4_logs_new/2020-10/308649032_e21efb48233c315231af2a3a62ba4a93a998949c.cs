using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FactureElectricite
{
    class Program
    {
        public static void saisie_client(out string nom, out string prenom, out string adr, out string ville, out string cp)
        {
            Console.WriteLine("entrez le nom du client");
            nom = Console.ReadLine();
            Console.WriteLine("entrez le prenom du client");
            prenom = Console.ReadLine();
            Console.WriteLine("entrez l'adresse du client");
            adr = Console.ReadLine();
            Console.WriteLine("entrez le code postal");
            cp = Console.ReadLine();
            Console.WriteLine("entrez la ville");
            ville = Console.ReadLine();
        }
        public static void edition_client(string nom, string prenom, string adr, string ville, string cp)
        {
            Console.WriteLine("Facture");
            Console.WriteLine();
            Console.WriteLine("Pour M " + nom + " " + prenom);
            Console.WriteLine(adr);
            Console.WriteLine(cp + " " + ville);
            Console.WriteLine();
            Console.WriteLine("Désignation    Pu    Q    Montant");
        }






        static void Main(string[] args)
        {
            string nome, prenome, adresse, cp, ville;
            int frais_fixes;
            double ancien_index, nouvel_index, conso, montant, total;
            saisie_client(out nome, out prenome, out adresse, out ville, out cp);
            Console.WriteLine("entrez l'ancien index?");
            ancien_index = double.Parse(Console.ReadLine());
            Console.WriteLine("entrez le nouvel  index?");
            nouvel_index = double.Parse(Console.ReadLine());
            conso = nouvel_index - ancien_index;
            frais_fixes = 25;
            if (conso <= 100)
                montant = conso * 0.20;
            else
            {
                if (conso <= 250)
                {
                    montant = (100 * 0.20) + ((conso - 100) * 0.35);
                }
                else
                {
                    montant = (100 * 0.20) + (150 * 0.35) + ((conso - 250) * 0.20);
                }
            }
            total = montant + frais_fixes;
            edition_client(nome, prenome, adresse, ville, cp);
            // edition de la facture 
            Console.WriteLine("consommation " + conso + " kwhs");
            Console.WriteLine(" le montant de votre facture s'élève à " + total);

            Console.ReadLine();
        }
    }
}