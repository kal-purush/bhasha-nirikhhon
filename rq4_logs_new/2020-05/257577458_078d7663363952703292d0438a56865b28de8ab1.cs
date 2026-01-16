using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace Monopoly_Model
{
    public class Pion
    {
        private string _omschrijving;
        private int _grootte;
        private string _kleur;

        public Pion(string kleur)
        {
            _grootte = 30;
            _kleur = kleur;
            BepaalOmschrijving();
        }

        private void BepaalOmschrijving()
        {
            if(_kleur.ToLower() == "rood")
            {
                _omschrijving = "Rode Pion";
            }

            else if (_kleur.ToLower() == "blauw")
            {
                _omschrijving = "blauwe Pion";
            }

            else if (_kleur.ToLower() == "groen")
            {
                _omschrijving = "Groene Pion";
            }

            else if (_kleur.ToLower() == "paars")
            {
                _omschrijving = "Paarse Pion";
            }

            else if (_kleur.ToLower() == "zwart")
            {
                _omschrijving = "Zwarte Pion";
            }

            else if(_kleur.ToLower() == "geel")
            {
                _omschrijving = "Gele Pion";
            }
        }

        public int Grootte { get => _grootte; set => _grootte = value; }
        public string Kleur { get => _kleur; set => _kleur = value; }
        public string Omschrijving { get => _omschrijving; set => _omschrijving = value; }

        public override string ToString()
        {
            return _omschrijving;
        }
    }
}