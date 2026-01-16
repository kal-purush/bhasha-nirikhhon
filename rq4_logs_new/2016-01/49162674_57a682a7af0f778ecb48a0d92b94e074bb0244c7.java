import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;

/**
 * Created by Immortan on 1/12/2016.
 */
public class CommandCard extends JPanel{

    private Game game;
    private ArrayList<JToggleButton> buttons;
    String[] buttonNames = {"Reverse Gravity","Bomb","Increase Scrolling",
            "Decrease Scrolling","Snare","Destroy Ledge","Drop minion"};
    public static final int REVERSE_GRAVITY = 1;
    public static final int BOMB = 2;
    public static final int UP_SCROLL_RATE = 3;
    public static final int DOWN_SCROLL_RATE = 4;
    public static final int SNARE = 5;
    public static final int REMOVE_LEDGE = 6;
    public static int activeButton = 0;

    /*






    ***   ***   ******   **    **
    ***   ***   **        **  **
    *********   ******     ****
    *********   ******      **
    ***   ***   **          **
    ***   ***   ******      **

    use get PrefferedSize() to create a seperate panel for this logic
    k thx









    */
    public CommandCard(Game game)
    {
        setLayout(null);
        this.game = game;
        this.setBounds(800,0,100,600);
        this.setVisible(true);
        this.setOpaque(true);
        //this.setFocusable(true);
        //setBackground(Color.green);
        createButtons();
    }

    private void createButtons()
    {
        buttons = new ArrayList<>(buttonNames.length);
        int i = 0;
        while(i<buttonNames.length)
        {
            JToggleButton button = new JToggleButton(buttonNames[i]);
            button.setEnabled(false);
            button.setBounds(2,i*600/buttonNames.length,50,50);
            button.addActionListener(e -> buttonPressed(button));
            buttons.add(button);
            this.add(button);
            if(i==REVERSE_GRAVITY)
                button.setEnabled(true);
            i++;
        }
        game.reFocus();
    }

    public void draw(Graphics g)
    {

    }

    private void buttonPressed(JToggleButton button)
    {
        button.setSelected(true);
        for(int i = 0;i<buttonNames.length;i++)
        {
            JToggleButton tempButton = buttons.get(i);
            if(!tempButton.equals(button)&&tempButton.isEnabled())
                tempButton.setSelected(false);
            else if(tempButton.equals(button))
                activeButton = i;
        }

        switch(activeButton)
        {
            case REVERSE_GRAVITY:
                reverseGravity();
                button.setSelected(false);
                break;

            case UP_SCROLL_RATE:
                changeScroll(true);
                break;
            case DOWN_SCROLL_RATE:
                changeScroll(false);
                break;
        }
        game.reFocus();
    }

    private void reverseGravity()
    {
        game.getHero().gravity*=-1;
    }

    private void changeScroll(boolean isIncrease)
    {

    }
}