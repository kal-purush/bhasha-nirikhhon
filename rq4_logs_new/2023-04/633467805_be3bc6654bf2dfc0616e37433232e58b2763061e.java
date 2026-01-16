package a_vegetable_garden.vue_controleur.vues.components;

import a_vegetable_garden.modele.Singleton;
import a_vegetable_garden.modele.legumes.Carotte;
import a_vegetable_garden.modele.legumes.Salade;
import a_vegetable_garden.modele.legumes.Varietes;
import a_vegetable_garden.vue_controleur.icon.IconNames;
import a_vegetable_garden.vue_controleur.icon.IconRepository;

import javax.swing.*;
import java.io.Serializable;

public class ButtonsInfosLegume implements Singleton, Serializable {
    private static ButtonsInfosLegume instance = null;
    private final JButton[] buttons = new JButton[Varietes.values().length];
    private final ImageIcon[] icones = new ImageIcon[Varietes.values().length];
    private final String[] noms = new String[Varietes.values().length];
    private final String[] prix = new String[Varietes.values().length];

    public ButtonsInfosLegume() { chargerButtons();}

    public static ButtonsInfosLegume getInstance() {
        if (instance == null) {
            instance = new ButtonsInfosLegume();
        }
        return instance;
    }

    public JButton[] getButtons() {
        return buttons;
    }
    private void chargerIcones(){
        //Ordre des varietes dans l'enum Varietes = ordre des icones dans l'enum IconNames
        for (int i = 0; i < Varietes.values().length; i++) {
            icones[i] = IconRepository.getInstance().getIcone(IconNames.values()[i]);
        }
    }

    private void chargerNoms(){
        //Ordre = ordre des variétés dans l'enum Varietes
        for (int i = 0; i < Varietes.values().length; i++) {
            noms[i] = Varietes.values()[i].toString();
        }
    }

    private void chargerPrix(){
        //Ordre = ordre des variétés dans l'enum Varietes
        prix[0] = Float.toString(Salade.PRICE);
        prix[1] = Float.toString(Carotte.PRICE);
    }

    private void chargerButtons() {
        chargerIcones();
        chargerNoms();
        chargerPrix();
        // Ordre des boutons dans le tableau = Ordre des variétés dans l'enum Varietes
        for (int i = 0; i < Varietes.values().length; i++) {
            buttons[i] = new JButton(noms[i]+" : "+prix[i]+" pièces", icones[i]);
            buttons[i].setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));
        }
    }
}
