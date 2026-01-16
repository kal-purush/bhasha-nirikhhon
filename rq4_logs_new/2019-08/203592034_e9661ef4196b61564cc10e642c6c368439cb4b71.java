package button;

import javax.swing.JButton;

public class Button {

    private int x;
    private int y;
    private JButton button;

    public Button(int x, int y, JButton button) {
        this.x = x;
        this.y = y;
        this.button = button;
    }

    public int getX() {
        return x;
    }

    public int getY() {
        return y;
    }

    public JButton getButton() {
        return button;
    }

}