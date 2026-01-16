using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Trabalho1
{
    public class Gramatica
    {
        public string ID;

        public struct Regular
        {
            public string Atual;
            public List<string> Proximos;
        }

        public List<string> SimbolosFinais = new List<string>();
        public List<string> SimbolosIntermediarios = new List<string>();
        public List<Regular> Producoes = new List<Regular>();

        public Gramatica()
        {
        }

        public Gramatica (string ID)
        {
            this.ID = ID;
        }

        public void AddSimbolo(string s)
        {
            if (!SimbolosIntermediarios.Contains(s) & !SimbolosFinais.Equals(s))
            {
                if (s.Equals(s.ToLower()))
                {
                    SimbolosFinais.Add(s);
                }
                else
                {
                    SimbolosIntermediarios.Add(s);
                }
            }
        }

        public void AddProducao (Regular p)
        {
            AddSimbolo(p.Atual);

            foreach (var s in p.Proximos)
            {
                AddSimbolo(s);
            }

            Producoes.Add(p);
        }
    }
}