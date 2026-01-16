using System.Collections.Generic;

namespace WpfApplication1
{
    public struct Capteur
    {
        public string type;
        public string id;
        public string description;
        public string box;
        public string lieu;
        public Grandeur grandeur;
        public Valeur valeur;
        public List<Seuil> seuils;
        
    }

    public struct Grandeur
    {
        public string name;
        public string unite;
        public string abreviation;
    }

    public struct Valeur
    {
        public string type;
        public string min;
        public string max;
    }

    public struct Seuil
    {
        public string description;
        public string valeur;
    }
}