using System.Collections.Generic;
using EtudeManyToMany.Core.Model;

namespace EtudeManyToMany.Core.Data
{
    public static class InitialModel
    {
        public static readonly List<Utilisateur> Utilisateurs = new List<Utilisateur>()
        {
            new Utilisateur
            {
                UtilisateurId = 1,
                Nom = "Dupont",
                Email = "jean.dupont@gmail.com",
                Phone = "0607080910"
            },
            new Utilisateur
            {
                UtilisateurId = 2,
                Nom = "Dujardin",
                Email = "pierre.dujardin@gmail.com",
                Phone = "0607880910"
            },
            new Utilisateur
            {
                UtilisateurId = 3,
                Nom = "Doe",
                Email = "john.doe@gmail.com",
                Phone = "0609090910"
            }
        };

        public static readonly List<Passager> Passagers = new List<Passager>()
        {
            new Passager
            {
                PassagerId = 1,
                UtilisateurId = 1 
            },
            new Passager
            {
                PassagerId = 2,
                UtilisateurId = 2 
            }
        };

        public static readonly List<Conducteur> Conducteurs = new List<Conducteur>()
        {
            new Conducteur
            {
                ConducteurId = 1,
                UtilisateurId = 1
            },
            new Conducteur
            {
                ConducteurId = 2,
                UtilisateurId = 3,
                Trajets = new List<Trajet>()
                {
                    new Trajet
                    {
                        LieuDepart = "Paris",
                        LieuArrivee = "Marseille",
                    }
                }
            }
        };

        public static readonly List<Trajet> Trajets = new List<Trajet>()
        {
            new Trajet
            {
                TrajetId = 1,
                LieuDepart = "Paris",
                LieuArrivee = "Marseille",
                ConducteurId = 2
            }
        };

        public static readonly List<Reservation> Reservations = new List<Reservation>()
        {
            new Reservation
            {
                TrajetId = 1,
                PassagerId = 1
            },
            new Reservation
            {
                TrajetId = 1,
                PassagerId = 2
            }
        };

        public static readonly List<Administrateur> Admins = new List<Administrateur>()
        {
            new Administrateur
            {
                AdministrateurId = 1,
                Nom = "Admin",
                Password = "PAss62!",
            }
        };
    }
}