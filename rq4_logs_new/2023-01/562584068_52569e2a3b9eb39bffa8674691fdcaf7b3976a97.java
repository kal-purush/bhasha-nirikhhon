package de.hebk.gui.normal;

import de.hebk.game.Config;
import de.hebk.gamemodes.Normal;
import de.hebk.gui.JImagePanel;
import de.hebk.gui.StartGui;
import de.hebk.sound.SoundManager;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;


public class NormalQuestionGUI {
    private StartGui startGui;
    private SoundManager soundManager;
    private Normal normal;
    private JPanel panel1;
    private JButton button1;
    private JButton button2;
    private JButton button3;
    private JButton button4;
    private JLabel questionLabel;
    private JButton telefonjokerButton;
    private JButton publikumsJokerButton;
    private JButton a5050JokerButton;
    private JLabel stagelabel;
    private JLabel moneylabel;
    private JPanel left;
    private JPanel right;
    private JPanel bottom;
    private JPanel middle;

    //, Question question, Joker[] joker, Normal normal//
    public NormalQuestionGUI(StartGui startGui,Normal normal, SoundManager soundManager) {
        this.startGui = startGui;
        this.normal = normal;
        this.soundManager = soundManager;

        JImagePanel p = new JImagePanel(new ImageIcon(Config.getBackground()).getImage(), new GridLayout());
        p.add(panel1);
        startGui.pack();

        //beschriften
        questionLabel.setText(normal.getQuestion().getBody());
        moneylabel.setText(String.valueOf(normal.getMoney()));
        stagelabel.setText(String.valueOf(normal.getStufe()));
        button1.setText(normal.getQuestion().getAnswers()[0]);
        button2.setText(normal.getQuestion().getAnswers()[1]);
        button3.setText(normal.getQuestion().getAnswers()[2]);
        button4.setText(normal.getQuestion().getAnswers()[3]);

        startGui.setContentPane(p);
        startGui.revalidate();
        startGui.repaint();

        button1.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                //normal.checkanswer(normal.getQuestion().getAnswers()[0]);
                normal.checkanswer(1);
            }
        });

        button2.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                //normal.checkanswer(normal.getQuestion().getAnswers()[1]);
                normal.checkanswer(2);
            }
        });

        button3.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                //normal.checkanswer(normal.getQuestion().getAnswers()[2]);
                normal.checkanswer(3);
            }
        });

        button4.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                //normal.checkanswer(normal.getQuestion().getAnswers()[3]);
                normal.checkanswer(4);
            }
        });
    }


    private void createUIComponents() {
        questionLabel = new JLabel("test");
        button1 = new JButton();
        button2 = new JButton();
        button3 = new JButton();
        button4 = new JButton();
    }
}