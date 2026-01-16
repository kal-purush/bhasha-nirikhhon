import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

class Magic{
    JFrame f1;
    Magic(){
        f1 = new JFrame("Dice Magic");
        f1.setResizable(false);
        JButton b1 = new JButton("Single Player");
        b1.setBounds(160,100,150,40);
        JButton b2 = new JButton("Double Player");
        b2.setBounds(160,200,150,40);
        b1.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                b1.setEnabled(false);
                b2.setEnabled(false);
               startSinglePlayer();
            }
        });
        b2.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                b1.setEnabled(false);
                b2.setEnabled(false);
                startDoublePlayer();
            }
        });
        f1.add(b1);
        f1.add(b2);
        f1.setSize(500,400);
        f1.setLayout(null);
        f1.setVisible(true);
    }

    public void startSinglePlayer(){
        JFrame f2 = new JFrame("Dice Magic - Single Player");
        f2.setResizable(false);
        JButton rollDice1 = new JButton("Roll Dice1");
        rollDice1.setBounds(120,150,100,40);
        JButton rollDice2 = new JButton("Roll Dice2");
        rollDice2.setBounds(240,150,100,40);
        f2.add(rollDice1);
        f2.add(rollDice2);
        f2.setSize(500,400);
        f2.setLayout(null);
        f2.setVisible(true);
    }

    public void startDoublePlayer(){
        JFrame f2 = new JFrame("Dice Magic - Double Player");
        f2.setResizable(false);
        JButton rollDice1 = new JButton("Roll Dice1");
        rollDice1.setBounds(120,150,100,40);
        JButton rollDice2 = new JButton("Roll Dice2");
        rollDice2.setBounds(240,150,100,40);
        f2.add(rollDice1);
        f2.add(rollDice2);
        f2.setSize(500,400);
        f2.setLayout(null);
        f2.setVisible(true);
    }
}

public class DiceMagic {
    public static void main(String[] args){
        new Magic();
    }
}