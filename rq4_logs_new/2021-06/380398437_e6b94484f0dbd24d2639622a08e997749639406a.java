import javax.swing.*;
import java.awt.*;
import java.awt.event.*;

class GloomCounterGUI implements ActionListener, MouseListener
{

    private JFrame frame;
    private JComboBox<Integer> hand, discard, lost;
    private JButton calc;
    private JLabel round, lostRound, handLabel, discardLabel, lostLabel;
    private Font resultFont = new Font("Century", Font.BOLD, 15 );
    private Color bgColor = new Color(247, 200, 96);
    private Color objectColor = new Color(166, 69, 13);
    private Color fontColor = new Color(66, 9, 0);
    private JTextField[] lostRounds;
    private final Integer[] NUM_LIST = {0, 1 , 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13};
    private int lostNum = 0;

    public GloomCounterGUI()
    {
        frame = new JFrame("Gloomhaven Round Counter");
        
        frame.setSize( 350, 500 );
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.getContentPane().setBackground(bgColor);
        frame.setLayout(null);

        hand = new JComboBox<Integer>(NUM_LIST);
        hand.setSize(60, 30);
        hand.setLocation(260,5);
        hand.addActionListener(this);
        hand.addMouseListener(this);
        hand.setBackground(objectColor);
        hand.setForeground(fontColor);

        handLabel = new JLabel("Cards in hand");
        handLabel.setSize( 200, 30 );
        handLabel.setLocation( 5, 5 );

        discard = new JComboBox<Integer>(NUM_LIST);
        discard.setSize( 60, 30 );
        discard.setLocation(260, 35);
        discard.addActionListener(this);
        discard.addMouseListener(this);
        discard.setBackground(objectColor);
        discard.setForeground(fontColor);

        discardLabel = new JLabel("Cards in discard");
        discardLabel.setSize( 120, 30);
        discardLabel.setLocation( 5, 35 );

        lost = new JComboBox<Integer>(NUM_LIST);
        lost.setSize( 60, 30 );
        lost.setLocation(260, 65);
        lost.addActionListener(this);
        lost.addMouseListener(this);
        lost.setBackground(objectColor);
        lost.setForeground(fontColor);

        lostLabel = new JLabel("How many lost cards will you play?");
        lostLabel.setSize( 250, 30 );
        lostLabel.setLocation( 5, 65);

        calc = new JButton("Calculate");
        calc.setSize(150, 30);
        calc.setLocation(60, 260);
        calc.addActionListener(this);

        round = new JLabel("0 rounds left.");
        round.setSize(250, 30);
        round.setLocation(60, 300);
        round.setForeground(fontColor);
        round.setFont( resultFont );
        
        lostRound = new JLabel("<html>In what rounds would you<br/>like to use lost cards?<html>");
        lostRound.setSize(250, 30);
        lostRound.setLocation(50, 100);
        lostRound.setForeground(fontColor);

        frame.add(hand);
        frame.add(handLabel);
        frame.add(discard);
        frame.add(discardLabel);
        frame.add(lost);
        frame.add(lostLabel);
        frame.add(calc);
        frame.add(lostRound);
        frame.add(round);
        frame.setVisible( true );
    }

    public void actionPerformed( ActionEvent check )
    {
        if( check.getSource() == lost )
        {
            if( lostNum != 0 )
            {
                for( int i = 0; i < lostRounds.length; ++i )
                {
                    frame.remove(lostRounds[i]);
                    //removes any previously added text box before adding more
                }
            }
            //If lost was set to anything other than zero, new text boxes added
            lostNum = (int)lost.getSelectedItem();

            if( lostNum != 0 )
            {
                lostRounds = new JTextField[lostNum];
                for( int i = 0; i < lostNum; ++i)
                {  
                    lostRounds[i] = new JTextField("0");
                    lostRounds[i].addActionListener(this);
                    lostRounds[i].setSize(30,30);
                    if( i < 7)
                    {
                        lostRounds[i].setLocation( (35*(i+1)), 150);
                    }
                    else
                    {
                        lostRounds[i].setLocation( (35*(i-6) ), 190);
                    }
                    frame.add(lostRounds[i]);
                }

            }
            frame.invalidate();
            frame.validate();
            frame.repaint();
        }


            
        int handNum = 0;
        handNum = (int)hand.getSelectedItem();

        if( handNum != 0 ) 
        {
            int discardNum = 0;
            int roundNum = 0;
            int[] lostArray;
            discardNum = (int)discard.getSelectedItem();
            
            if( lostNum != 0 )
            {
                lostArray = new int[lostNum];
                for( int i =0; i < lostRounds.length; ++i)
                {
                    lostArray[i] = Integer.parseInt( lostRounds[i].getText() );
                }

            }
            else   
            {
                lostArray = new int[0];
            }
            roundNum = GloomHavenRoundCount.roundCount(handNum, discardNum, lostArray);
            round.setText( Integer.toString(roundNum) + " rounds left." );
        }
        
    }
    @Override
    public void mouseClicked( MouseEvent e )
    {    }

    public void mousePressed( MouseEvent e)
    {}
    
    public void mouseReleased( MouseEvent e )
    {}

    public void mouseExited( MouseEvent e )
    {}

    public void mouseEntered( MouseEvent e )
    {}

    
    

    public static void main(String[] args)
    {

        GloomCounterGUI runIt = new GloomCounterGUI();

    }
}