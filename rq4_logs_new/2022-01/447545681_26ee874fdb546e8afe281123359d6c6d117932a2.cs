using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Xna.Framework;

namespace lost_clothes_code
{
    public class Perso
    {
        private int hauteur;
        private int largeur;
        private int vitesseDeplacement;
        private int vitesseMarche;
        private Vector2 position;

        public Perso(int hauteur, int largeur, Vector2 position)
        {
            this.hauteur = hauteur;
            this.largeur = largeur;
            this.position = position;
        }

        public int Hauteur
        {
            get
            {
                return this.hauteur;
            }
            set
            {
                this.hauteur = value;
            }
        }

        public int Largeur
        {
            get
            {
                return this.largeur;
            }
            set
            {
                this.largeur = value;
            }
        }

        public Vector2 Position
        {
            get
            {
                return this.position;
            }
            set
            {
                this.position = value;
            }
        }

        public int VitesseDeplacement { get => vitesseDeplacement; set => vitesseDeplacement = value; }
        public int VitesseMarche { get => vitesseMarche; set => vitesseMarche = value; }
    }
}