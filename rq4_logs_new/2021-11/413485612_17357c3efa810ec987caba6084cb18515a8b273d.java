package GUI_JFRAME;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class LaunchPage implements ActionListener {
    JFrame frame = new JFrame();
    JButton chess = new JButton("Chess");
    JButton SuperChess = new JButton("SuperChess");

    LaunchPage(){
        chess.setBounds(150,160, 200, 40);
        chess.setFocusable(false);
        chess.addActionListener(this);

        SuperChess.setBounds(150, 210, 200, 40);
        SuperChess.setFocusable(false);
        SuperChess.addActionListener(this);

        frame.add(chess);
        frame.add(SuperChess);
        frame.setTitle("Welcome!");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setSize(500, 500);
        frame.setLayout(null);
        frame.setVisible(true);
    }

    @Override
    public void actionPerformed(ActionEvent e) {
        //Set the Action of clicking Chess Button, jump to GUI_ChessBoard
        if(e.getSource()==chess) {
            GUI_ChessBoard chess = new GUI_ChessBoard();
            chess.display();
            frame.setVisible(false);
        }
        //Set the action of clicking Super_Chess Button, jump to Super_Board.
        if(e.getSource()==SuperChess) {
            SuperBoard SuperChess = new SuperBoard();
            frame.setVisible(false);
        }
    }
}