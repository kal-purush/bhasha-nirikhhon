package game;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JTextField;

public class AllenSucks extends JFrame implements ActionListener
{
    private JButton  start;

    private JTextField name;
    private JLabel label;

    private GameMain myMain;

    public AllenSucks(GameMain main)
    {
        myMain = main;
        this.setSize(300, 300);
        label = new JLabel ("Enter Name:");
        name = new JTextField();
        start = new JButton("start game");

        label.setBounds(80, 60, 130, 30);
        name.setBounds(	80, 60, 130, 30);
        start.setBounds(100, 190, 100, 40);
        start.addActionListener(this);
        this.add(start);
        this.add(name);
        this.add(label);

        this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        this.setTitle("allen?");
        this.setVisible(true);
    }

    @Override
    public void actionPerformed(ActionEvent e)
    {
        myMain.startGame();
        this.dispose();
    }
}