package com.dice.dicemagic;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.geom.RoundRectangle2D;

public class DiceMagic{
    JFrame mainWindow;
    Image appIcon = Toolkit.getDefaultToolkit().getImage("img/dice-magic-logo.png");
    DiceMagic(){
        mainWindow = new JFrame("Dice Magic");

        JButton minimizeButton = new JButton(new ImageIcon("img/minimize.png"));
        minimizeButton.setBounds(440,20,20,20);
        minimizeButton.setCursor(new Cursor(Cursor.HAND_CURSOR));
        minimizeButton.setBorderPainted(false);
        minimizeButton.setContentAreaFilled(false);
        minimizeButton.setFocusPainted(false);
        minimizeButton.setOpaque(false);

        JButton closeButton = new JButton(new ImageIcon("img/close.png"));
        closeButton.setBounds(470,20,20,20);
        closeButton.setCursor(new Cursor(Cursor.HAND_CURSOR));
        closeButton.setBorderPainted(false);
        closeButton.setContentAreaFilled(false);
        closeButton.setFocusPainted(false);
        closeButton.setOpaque(false);

        JLabel playMessage = new JLabel("Play!");
        playMessage.setBounds(230, 40, 150, 40);
        playMessage.setFont(new Font(playMessage.getFont().getName(), Font.BOLD, 24));
        playMessage.setForeground(new Color(45, 131, 148));

        ImageIcon diceTitleImage = new ImageIcon("img/dice-title.png");
        JLabel diceTitle = new JLabel();
        diceTitle.setBounds(100,90,131,98);
        diceTitle.setIcon(diceTitleImage);
        ImageIcon diceImage = new ImageIcon("img/dice.gif");
        JLabel diceAnimation = new JLabel();
        diceAnimation.setBounds(215,140,72,72);
        diceAnimation.setIcon(diceImage);
        ImageIcon magicTitleImage = new ImageIcon("img/magic-title.png");
        JLabel magicTitle = new JLabel();
        magicTitle.setBounds(277,175,156,98);
        magicTitle.setIcon(magicTitleImage);

        JButton onePlayerButton = new JButton("1 Player");
        onePlayerButton.setBounds(190, 310, 120, 40);
        onePlayerButton.setCursor(new Cursor(Cursor.HAND_CURSOR));
        onePlayerButton.setContentAreaFilled(false);
        onePlayerButton.setFocusPainted(false);
        onePlayerButton.setOpaque(false);

        JButton twoPlayerButton = new JButton("2 Player");
        twoPlayerButton.setBounds(190, 380, 120, 40);
        twoPlayerButton.setCursor(new Cursor(Cursor.HAND_CURSOR));
        twoPlayerButton.setContentAreaFilled(false);
        twoPlayerButton.setFocusPainted(false);
        twoPlayerButton.setOpaque(false);

        DragListener drag = new DragListener();
        mainWindow.addMouseListener(drag);
        mainWindow.addMouseMotionListener(drag);

        minimizeButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                mainWindow.setState(Frame.ICONIFIED);
            }
        });

        closeButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                if (JOptionPane.showConfirmDialog(mainWindow,
                        "Are you sure you want to close this application?",
                        "Closing Dice Magic?",
                        JOptionPane.YES_NO_OPTION,
                        JOptionPane.QUESTION_MESSAGE,
                        new ImageIcon("img/cross.png")) == JOptionPane.YES_OPTION) {
                    System.exit(0);
                } else {
                    mainWindow.setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE);
                }
            }
        });

        onePlayerButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                mainWindow.setVisible(false);
                startOnePlayer();
            }
        });

        twoPlayerButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                mainWindow.setVisible(false);
                startTwoPlayer();
            }
        });

        mainWindow.add(minimizeButton);
        mainWindow.add(closeButton);
        mainWindow.add(playMessage);
        mainWindow.add(diceTitle);
        mainWindow.add(diceAnimation);
        mainWindow.add(magicTitle);
        mainWindow.add(onePlayerButton);
        mainWindow.add(twoPlayerButton);
        mainWindow.setIconImage(appIcon);
        mainWindow.setSize(550, 500);
        mainWindow.setUndecorated(true);
        mainWindow.setShape(new RoundRectangle2D.Double(10,10,500,450,50,50));
        mainWindow.getContentPane().setBackground(new Color(245,245,245));
        mainWindow.setLayout(null);
        mainWindow.setResizable(false);
        mainWindow.setLocationRelativeTo(null);
        mainWindow.setVisible(true);
    }

    public void startOnePlayer(){
        JFrame onePlayerWindow = new JFrame("Dice Magic - 1 Player");
        JButton homeButton = new JButton(new ImageIcon("img/home.png"));
        homeButton.setBounds(450,10,32,32);
        homeButton.setCursor(new Cursor(Cursor.HAND_CURSOR));
        homeButton.setBorderPainted(false);
        homeButton.setContentAreaFilled(false);
        homeButton.setFocusPainted(false);
        homeButton.setOpaque(false);
        JButton rollDice1 = new JButton("Roll Dice 1");
        rollDice1.setBounds(120,150,100,40);
        rollDice1.setCursor(new Cursor(Cursor.HAND_CURSOR));
        JButton rollDice2 = new JButton("Roll Dice 2");
        rollDice2.setBounds(240,150,100,40);
        rollDice2.setCursor(new Cursor(Cursor.HAND_CURSOR));
        onePlayerWindow.add(homeButton);
        onePlayerWindow.add(rollDice1);
        onePlayerWindow.add(rollDice2);
        onePlayerWindow.setSize(500,400);
        onePlayerWindow.setIconImage(appIcon);
        onePlayerWindow.getContentPane().setBackground(Color.WHITE);
        onePlayerWindow.setUndecorated(true);
        onePlayerWindow.setLayout(null);
        onePlayerWindow.setResizable(false);
        onePlayerWindow.setLocationRelativeTo(null);
        onePlayerWindow.setVisible(true);

        DragListener drag = new DragListener();
        onePlayerWindow.addMouseListener(drag);
        onePlayerWindow.addMouseMotionListener(drag);

       homeButton.addActionListener(new ActionListener() {
           @Override
           public void actionPerformed(ActionEvent e) {
               if (JOptionPane.showConfirmDialog(onePlayerWindow,
                       "Note: All your progress will be lost.",
                       "Go to Home",
                       JOptionPane.YES_NO_OPTION, JOptionPane.QUESTION_MESSAGE,
                       new ImageIcon("img/bin.png")) == JOptionPane.YES_OPTION){
                   mainWindow.setVisible(true);
                   onePlayerWindow.dispose();
               } else {
                   onePlayerWindow.setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE);
               }
           }
       });
    }

    public void startTwoPlayer(){
        JFrame twoPlayerWindow = new JFrame("Dice Magic - 2 Player");
        JButton homeButton = new JButton(new ImageIcon("img/home.png"));
        homeButton.setBounds(450,10,32,32);
        homeButton.setCursor(new Cursor(Cursor.HAND_CURSOR));
        homeButton.setBorderPainted(false);
        homeButton.setContentAreaFilled(false);
        homeButton.setFocusPainted(false);
        homeButton.setOpaque(false);
        JButton rollDice1 = new JButton("Roll Dice 1");
        rollDice1.setBounds(120,150,100,40);
        rollDice1.setCursor(new Cursor(Cursor.HAND_CURSOR));
        JButton rollDice2 = new JButton("Roll Dice 2");
        rollDice2.setBounds(240,150,100,40);
        rollDice2.setCursor(new Cursor(Cursor.HAND_CURSOR));
        twoPlayerWindow.add(homeButton);
        twoPlayerWindow.add(rollDice1);
        twoPlayerWindow.add(rollDice2);
        twoPlayerWindow.setIconImage(appIcon);
        twoPlayerWindow.setSize(500,600);
        twoPlayerWindow.getContentPane().setBackground(Color.WHITE);
        twoPlayerWindow.setUndecorated(true);
        twoPlayerWindow.setLayout(null);
        twoPlayerWindow.setResizable(false);
        twoPlayerWindow.setLocationRelativeTo(null);
        twoPlayerWindow.setVisible(true);

        DragListener drag = new DragListener();
        twoPlayerWindow.addMouseListener(drag);
        twoPlayerWindow.addMouseMotionListener(drag);

        homeButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                if (JOptionPane.showConfirmDialog(twoPlayerWindow,
                        "Note: All your progress will be lost.",
                        "Go to Home",
                        JOptionPane.YES_NO_OPTION,
                        JOptionPane.QUESTION_MESSAGE,
                        new ImageIcon("img/bin.png")) == JOptionPane.YES_OPTION){
                    mainWindow.setVisible(true);
                    twoPlayerWindow.dispose();
                } else {
                    twoPlayerWindow.setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE);
                }
            }
        });
    }

    public int generateRandomNumber(String s){
        if (s.equals("challenge")){
            return (int)(Math.random()*(12-2) + 2);
        } else if (s.equals("rollDice")){
            return (int)(Math.random()*(6-1) + 1);
        }
        return -1;
    }

    public static void main(String[] args) {
        new DiceMagic();
    }
}