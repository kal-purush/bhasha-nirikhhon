package ObserverObservableMVC.Model.EasyMemento;

import AbstractFactories.FormeFactories.Point;
import Builder.Decorator.Animal;
import Builder.Decorator.AnimalAvecMouvement;
import ObserverObservableMVC.Model.AbstractModel;
import ObserverObservableMVC.Observer;

import java.util.ArrayList;

public abstract class Jeu extends AbstractModel {

    protected Jeu state;

    public Jeu getState() {
        return state;
    }

    public void setState(Jeu State) {
        this.state = State;
    }


    /**
     * Définit les animaux qui sont soit vivants soit morts
     *
     * @param Animaux
     */
    public void setAnimaux(ArrayList<Animal> Animaux) {
        animaux = Animaux;
    }


    public abstract void init();

    public Boolean distance(AnimalAvecMouvement animalAvecMouvement, Animal animal) {
        java.awt.Point box1 = animal.getPoint().getPoints().get(0);
        int height1 = animal.getImageIcon().getIconHeight();
        int width1 = animal.getImageIcon().getIconWidth();
        java.awt.Point box2 = animalAvecMouvement.getPoint().getPoints().get(0);
        int height2 = animalAvecMouvement.getImageIcon().getIconHeight();
        int width2 = animalAvecMouvement.getImageIcon().getIconHeight();

        if ((box2.x >= box1.x + width1)      // trop à droite
                || (box2.x + width2 <= box1.x) // trop à gauche
                || (box2.y >= box1.y + height1) // trop en bas
                || (box2.y + height2 <= box1.y))  // trop en haut
            return false;
        else
            return true;
    }

  /*bool Collision(AABB box1,AABB box2)
  {
    if(         (box2.x >= box1.x + box1.w)      // trop à droite
            ||  (box2.x + box2.w <= box1.x) // trop à gauche
            ||  (box2.y >= box1.y + box1.h) // trop en bas
            ||  (box2.y + box2.h <= box1.y))  // trop en haut
      return false;
    else
      return true;
  }*/

    public void stop() {
    }

    public void run() {
    }

    public void calcul() {
    }

    /**
     * met à vide les liste  animaux et animauxAvecMouvement
     */
    public void reset() {
        animaux = new ArrayList<>();
        animauxAvecMouvement = new ArrayList<>();
    }


    /**
     * Affichage forcé de la fenêtre pour éviter les scintillements
     */
    public abstract void update();

    /**
     * Définit les animauxAvecMouvement qui touchent ou non les animaux
     *
     * @param AnimauxAvecMouvement
     */
    public void setAnimauxAvecMouvement(ArrayList<AnimalAvecMouvement> AnimauxAvecMouvement) {
        animauxAvecMouvement = AnimauxAvecMouvement;
    }
}