package user_interface;

import javax.swing.JButton;
import javax.swing.JFrame;

// make a starting frame that asks for the game mode with one button only
// after clicking the button switch to the chessFrame
// make a new class that extends JFrame and has a button
public class StartFrame extends JFrame {
    private ChessFrame chessFrame;

    public StartFrame(ChessFrame chessFrame) {
        super("Chess");
        this.chessFrame = chessFrame;
        // make it bigger with better font
        JButton button = new JButton("Start Double Player Game");
        button.setBounds(100, 100, 300, 300);
        button.addActionListener(e -> {
            dispose();
            this.chessFrame.setVisible(true);
        });
        add(button);

        setLayout(null);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setSize(500, 500);
        setLocationRelativeTo(null);
        setVisible(true);
    }
}