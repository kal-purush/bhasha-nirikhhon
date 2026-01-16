using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Lab3
{
    internal class Employe : IEquatable<Employe>, INotifyPropertyChanged
    {
        string matricule;
        string nom;
        string prenom;

        public Employe(string matricule, string nom, string prenom)
        {
            this.matricule = matricule;
            this.nom = nom;
            this.prenom = prenom;
        }

        public string Matricule
        {
            get { return matricule; }
            set { matricule = value; this.OnPropertyChanged(); }

        }
        public string Prenom
        {
            get { return prenom; }
            set { prenom = value; this.OnPropertyChanged(); }
        }

        public string Nom
        {
            get { return nom; }
            set { nom = value; this.OnPropertyChanged(); }
        }

         bool IEquatable<Employe>.Equals(Employe other)
        {
            if(this.matricule.Equals(other.matricule) && this.prenom.Equals(other.prenom) && this.nom.Equals(other.nom))
                return true;
            else 
                return false;
        }

        public override string ToString()
        {
            return matricule + " " + nom + " " + prenom;
        }

        public event PropertyChangedEventHandler PropertyChanged;

        public void OnPropertyChanged([CallerMemberName] string propertyName = null) =>
        this.PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
    }
}