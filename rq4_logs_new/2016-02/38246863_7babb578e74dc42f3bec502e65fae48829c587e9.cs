using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SecretOfGaia;
using System.Collections.Generic;

namespace SecretOfGaia_Test
{
    [TestClass]
    public class RuleController_Test
    {

        public Carte getCarte(string NomCarte)
        {
            switch (NomCarte)
            {
                case "Guêpe":
                    return new Carte("Guêpe", TypeCarte.Instantanee, 5, 0, 5);
                case "Mante religieuse":
                    return new Carte("Mante religieuse", TypeCarte.Instantanee, 3, 0, 2);
                case "Goéland leucophée":
                    return new Carte("Goéland leucophée", TypeCarte.Instantanee, 4, 0, 2);
                default:
                    return new Carte("Guêpe", TypeCarte.Instantanee, 5, 0, 4);
            }
        }


        public Deck construireUnDeck(Dictionary<string, int> ListeCarte)
        {
            Deck monDeck = new Deck();
            foreach (string clef in ListeCarte.Keys)
            {
                for (int i = 0; i < ListeCarte[clef]; i++)
                {
                    monDeck.ajouterCarte(getCarte(clef));
                }
            }
            return monDeck;

        }
        public Joueur obtenirJoueur1()
        {

            Deck DeckJoueur1 = construireUnDeck(new Dictionary<string, int>{
                {"Guêpe",5},
                {"Mante religieuse",10},
                {"Goéland leucophée",3}
            });



            Dictionary<string, CaracteristiqueJoueur> caracs = new Dictionary<string, CaracteristiqueJoueur> {
                {"Force",new CaracteristiqueJoueur(5)}
                ,{"PV",new CaracteristiqueJoueur(15)}
            };
            Joueur MonJoueur1 = new Joueur("JoueurTest1", curCaracs: caracs, curDecks: new List<Deck> { DeckJoueur1 });
            return MonJoueur1;
        }

        public Joueur obtenirJoueur2()
        {
            Deck DeckJoueur2 = construireUnDeck(new Dictionary<string, int>{
                {"Guêpe",10},
                {"Mante religieuse",5},
                {"Goéland leucophée",5}
            });

            Dictionary<string, CaracteristiqueJoueur> caracs = new Dictionary<string, CaracteristiqueJoueur> {
                {"Force",new CaracteristiqueJoueur(8)}
                ,{"PV",new CaracteristiqueJoueur(22)}
            };
            Joueur MonJoueur = new Joueur("JoueurTest1", curCaracs: caracs, curDecks: new List<Deck> { DeckJoueur2 });
            return MonJoueur;
        }

        [TestMethod]
        public void TestDebutDuJeu()
        {
            RuleController MonControlleur = new RuleController();
            MonControlleur.demarerDuel(obtenirJoueur1(), obtenirJoueur2());
            Assert.AreEqual(1, MonControlleur.numTour, "Num tour NOK");
            Assert.AreEqual(3, MonControlleur.nbAction, "NbAction tour NOK");
            Assert.AreEqual(4, MonControlleur.joueur1.cartesEnMain.Count, "Nb cartes en mains joueur 1 NOk");
            Assert.AreEqual(4, MonControlleur.joueur2.cartesEnMain.Count, "Nb cartes en mains joueur 2 NOk");

        }

        [TestMethod]
        public void TestJouerCartePossible()
        {
            RuleController MonControlleur = new RuleController();
            MonControlleur.demarerDuel(obtenirJoueur1(), obtenirJoueur2());
            decimal actionAvantCarte = MonControlleur.joueur1["actions"];
            decimal PVJoueur2 = MonControlleur.joueur2["PV"];
            MonControlleur.joueur1.cartesEnMain.ajouterCarte(getCarte("Mante religieuse")) ;
            Assert.AreEqual(5, MonControlleur.joueur1.cartesEnMain.Count, "Nb cartes en mains joueur 1 NOk");
            Carte carteJouee = MonControlleur.jouerUneCarteDepuisLaMain("Mante religieuse");

            Assert.AreEqual(actionAvantCarte - carteJouee.action, MonControlleur.joueur1["actions"], "Nb action après jouer une carte NOK");
            Assert.AreEqual(4, MonControlleur.joueur1.cartesEnMain.Count, "Nb cartes en mains joueur 1 après jouer une carte NOk");
            Assert.AreEqual(PVJoueur2 - carteJouee.attaque, MonControlleur.joueur2["PV"], "PV joueur 2  après jouer une carte NOk");


        }


        [TestMethod]
        public void TestJouerCarteImpossible()
        {
            RuleController MonControlleur = new RuleController();
            MonControlleur.demarerDuel(obtenirJoueur1(), obtenirJoueur2());
            decimal actionAvantCarte = MonControlleur.joueur1["actions"];
            decimal PVJoueur2 = MonControlleur.joueur2["PV"];
            MonControlleur.joueur1.cartesEnMain.ajouterCarte(getCarte("Guêpe"));
            Assert.AreEqual(5, MonControlleur.joueur1.cartesEnMain.Count, "Nb cartes en mains joueur 1 NOk");
            Carte carteJouee = MonControlleur.jouerUneCarteDepuisLaMain("Guêpe");


            Assert.AreEqual(actionAvantCarte, MonControlleur.joueur1["actions"], "Nb action après jouer une carte NOK");
            Assert.AreEqual(5, MonControlleur.joueur1.cartesEnMain.Count, "Nb cartes en mains joueur 1 après jouer une carte NOk");
            Assert.AreEqual(PVJoueur2,MonControlleur.joueur2["PV"], "PV joueur 2  après jouer une carte NOk");
        }


        /*
        [TestMethod]
        public void TestAjoutCarteEnTrop()
        {
            Terrain curPose = new Terrain(2);
            Assert.AreEqual(curPose.Count, 0, "Création de Terrain NOK");
            Carte maCarte1 = new Carte("Carte1", TypeCarte.Instantanee, 1, 1, 12);
            bool AjoutOK =  curPose.ajouterCarte(maCarte1);
            Carte maCarte2 = new Carte("Carte2", TypeCarte.Instantanee, 1, 1, 7);
            AjoutOK = curPose.ajouterCarte(maCarte2);
            Assert.AreEqual(true, AjoutOK, "Ajout Carte autorise NOK sur retour ajouterCarte");
            Assert.AreEqual(2, curPose.Count, "Ajout de 2 Cartes NOK");
            Assert.AreEqual(maCarte2, curPose[2], "Ajout 2éme carte pas à la bonne position ");
            Assert.AreEqual(maCarte1, curPose.prochaineCarte, "Prochaine Carte NOK avec 2 éléements ");
            Carte maCarte3 = new Carte("Carte3", TypeCarte.Instantanee, 1, 1, 7);
            AjoutOK = curPose.ajouterCarte(maCarte3);
            Assert.AreEqual(false, AjoutOK, "Ajout Carte Interdite NOK sur retour ajouterCarte");
            Assert.AreEqual(2, curPose.Count, "Ajout de 2 Cartes NOK sur Count");

        }

        [TestMethod]
        public void TestAjoutCarteSuperposée()
        {
            Terrain curPose = new Terrain(2);
            Carte maCarte1 = new Carte("Carte1", TypeCarte.Instantanee, 1, 1, 12);
            bool AjoutOK = curPose.ajouterCarte(maCarte1);
            Carte maCarte2 = new Carte("Carte2", TypeCarte.Instantanee, 1, 1, 7);
            AjoutOK = curPose.poserSur(1,maCarte2);
            Assert.AreEqual(1, curPose.Count, "Ajout de Cartes dessus NOK");
            Assert.AreEqual(maCarte2, curPose[1], "Ajout de Cartes dessus NOK");

        }


        [TestMethod]
        public void TestElenverCarteSuperposée()
        {
            Terrain curPose = new Terrain(2);
            Carte maCarte1 = new Carte("Carte1", TypeCarte.Instantanee, 1, 1, 12);
            bool AjoutOK = curPose.ajouterCarte(maCarte1);
            Carte maCarte2 = new Carte("Carte2", TypeCarte.Instantanee, 1, 1, 7);
            AjoutOK = curPose.poserSur(1, maCarte2);
            curPose.enleverCarte(1);
            Assert.AreEqual(1, curPose.Count, "Enlever Carte dessus Count NOK");
            Assert.AreEqual(maCarte1, curPose[1], "Enlever Cartes dessus Carte NOK");
        }

        [TestMethod]
        public void TestElenverCarteduDessousAvecSuperposition()
        {
            Terrain curPose = new Terrain(2);
            Carte maCarte1 = new Carte("Carte1", TypeCarte.Instantanee, 1, 1, 12);
            bool AjoutOK = curPose.ajouterCarte(maCarte1);
            Carte maCarte2 = new Carte("Carte2", TypeCarte.Instantanee, 1, 1, 7);
            AjoutOK = curPose.poserSur(1, maCarte2);
            curPose.enleverCarte(1,true);
            Assert.AreEqual(0, curPose.Count, "Enlever Carte dessous Count NOK");
        }*/
    }
}