package menu;

import javafx.event.EventHandler;
import javafx.scene.Group;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.input.MouseEvent;
import javafx.scene.layout.GridPane;
import javafx.scene.paint.Color;
import jeux.Main;
import menu.menuBoutique.MenuBoutique;
import sample.SnakeJeu;

import static sample.SnakeJeu.Hauteur;
import static sample.SnakeJeu.Largeur;

public class MenuPrincipal extends Scene {
    Button[] boutons;
    Son son;
    Image image;
    Clavier mon_clavier;
    ImageView iv;

    public MenuPrincipal(){
        super(new Group(),800,600);
        initAttribut();
        ajouterAddAtribut();
        addMouseEvent();
    }
    public void initAttribut(){
        son = new Son();
        boutons = new Button[]{
                new Button("Snake"),
                new Button("Boutique")
        };

        image = new Image("file:Background.png");
        iv= new ImageView();
    }
    public void ajouterAddAtribut(){
        Group root = (Group)this.getRoot();
        GridPane gp = new GridPane();
        gp.addColumn(0,boutons);
        root.getChildren().add(gp);

        iv.setImage(image);
        iv.setFitWidth(800);
        iv.setFitHeight(600);

        root.getChildren().add(iv);

    }

    public void addMouseEvent() {

        boutons[0].setOnMouseClicked(new EventHandler<MouseEvent>() {
            @Override
            public void handle(MouseEvent event) {
                System.out.println("btest");
                Main.getStage().setScene(new SnakeJeu());

            }
        });

        boutons[1].setOnMouseClicked(new EventHandler<MouseEvent>() {
            @Override
            public void handle(MouseEvent event) {
                System.out.println("btest");
                Main.getStage().setScene(new MenuBoutique());
            }
        });
    }
}