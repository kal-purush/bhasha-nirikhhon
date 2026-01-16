package npcgenerator;

import javax.swing.BorderFactory;
import javax.swing.border.*;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import java.awt.GridLayout;


public class NPCgenerator extends JFrame {
    
    private JPanel mainPanel, genderPanel, racePanel, npcPanel;
    private JRadioButton male, female, na, human, cambion, changeling, clockwork, dwarf,
            elf, faun, goblin, hobgoblin, jotun, orc, pixie, revenant, salamander;
    private Border kehys;
    private TitledBorder genderBorder, raceBorder;
    
    public NPCgenerator() {
        setTitle("NPCs, bit*es!");
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        mainPanel = new JPanel();
        genderPanel = new JPanel(new GridLayout(0,1));
        racePanel = new JPanel(new GridLayout(0,5));
        
        male = new JRadioButton("Male");
        female = new JRadioButton("Female");
        na = new JRadioButton("N/A");
        
        human = new JRadioButton("Human");
        cambion = new JRadioButton("Cambion");
        changeling = new JRadioButton("Changeling");
        clockwork = new JRadioButton("Clockwork");
        dwarf = new JRadioButton("Dwarf");
        faun = new JRadioButton("Faun");
        elf = new JRadioButton("Elf");
        goblin = new JRadioButton("Goblin");
        hobgoblin = new JRadioButton("Hobgoblin");
        jotun = new JRadioButton("Jotun");
        orc = new JRadioButton("Orc");
        pixie = new JRadioButton("Pixie");
        revenant = new JRadioButton("Revenant");
        salamander = new JRadioButton("Salamander");
        
        genderPanel.add(male);
        genderPanel.add(female);
        genderPanel.add(na);
        kehys = BorderFactory.createEtchedBorder();
        genderBorder = BorderFactory.createTitledBorder(kehys, "Gender");
        genderPanel.setBorder(genderBorder);
        
        racePanel.add(human);
        racePanel.add(cambion);
        racePanel.add(changeling);
        racePanel.add(clockwork);
        racePanel.add(dwarf);
        racePanel.add(elf);
        racePanel.add(faun);
        racePanel.add(goblin);
        racePanel.add(hobgoblin);
        racePanel.add(jotun);
        racePanel.add(orc);
        racePanel.add(pixie);
        racePanel.add(revenant);
        racePanel.add(salamander);
        
        raceBorder = BorderFactory.createTitledBorder(kehys, "Race");
        racePanel.setBorder(raceBorder);
        
        mainPanel.add(genderPanel);
        mainPanel.add(racePanel);
        
        setContentPane(mainPanel);
        pack();
        setVisible(true);
    }
    
    public static void main(String[] args) {
        new NPCgenerator();
        
    }
    
}