import java.awt.*;
import java.awt.event.*;
import java.applet.*;
import javax.swing.*;
public class TwentyFourtyEight extends Applet implements ActionListener

{
    JButton[] a;
    int row = 4;
    int col = 4;
    int total = row * col;
    JLabel game;

    Panel p_card; //to hold all of the cards
    Panel card1, card2, card3;
    CardLayout cdLayout = new CardLayout ();

    public void init ()
    {
	p_card = new Panel ();
	p_card.setLayout (cdLayout);
	open ();
	grid ();
	resize (450, 330);
	setLayout (new BorderLayout ());
	add ("Center", p_card);
    }


    public void open ()
    {
	card1 = new Panel ();
	card1.setBackground (Color.pink);

	JLabel title = new JLabel ("2048");
	title.setFont (new Font ("Arial", Font.BOLD, 34));
	title.setForeground (Color.magenta);

	JLabel pic = new JButton (createImageIcon ("");

	JButton play = new JButton ("Play");
	play.addActionListener (this);
	play.setActionCommand ("p");
	play.setForeground (Color.pink);
	play.setBackground (Color.magenta);

	card1.add (title);
	card1.add (pic);
	card1.add (play);
	p_card.add ("1", card1);

    }


    public void grid ()
    {
	card1 = new Panel ();
	card1.setBackground (Color.pink);

	Panel p = new Panel (new GridLayout (row, col, 0, 0));
	resize (500, 400);
	a = new JButton [total];

	a [0] = new JButton (createImageIcon ("1.jpg"));
	p.add (a [0]);

	for (int i = 1 ; i < total - 1 ; i++)
	{
	    a [i] = new JButton ("");
	    p.add (a [i]);
	    a [i].addActionListener (this);
	    a [i].setBackground (Color.white);
	    a [i].setActionCommand ("" + i);
	}
	card2.add (p);

	JButton up = new JButton ("Up");
	up.addActionListener (this);
	up.setActionCommand ("u");

	JButton down = new JButton ("Down");
	down.addActionListener (this);
	down.setActionCommand ("d");

	JButton left = new JButton ("Left");
	left.addActionListener (this);
	left.setActionCommand ("l");

	JButton right = new JButton ("Right");
	right.addActionListener (this);
	right.setActionCommand ("r");


	JButton reset = new JButton ("Play Again?");
	reset.addActionListener (this);
	reset.setActionCommand ("-1");

	JLabel ins = new JLabel ("Use your arrow keys to move the tiles. When two tiles with the same number touch, they merge into one!");

	card2.add (reset);
	card2.add (count);
	card2.add (ins);
	card2.add (reset);
	p_card.add ("2", card2);
    }


    public void actionPerformed (ActionEvent e)
    {
	if (e.getActionCommand ().equals ("p"))
	{
	    cdLayout.show (p_card, "2");
	}

	else
	{
	    if (e.getActionCOmman ().equals ("l"))
	    {
		for (int i = 0 ; i < 15 ; i++)
		{
		    if ((a [i] == a [i - 1] && i != 4) || (a [i] == a [i - 1] && i != 8) || (a [i] == a [i - 1] && || i != 12))
		    {
			int t = a [i];
			int g = a [i - 1];
			if (t == g)
			{
			    int n = Integer.parseInt (e.getActionCommand ());
			    if (n.equals ("2.jpg"))
				a [i - 1].setIcon ("4.jpg");
			    else if (n.equals ("4.jpg"))
				a [i - 1].setIcon ("8.jpg");
			    else if (n.equals ("8.jpg"))
				a [i - 1].setIcon ("16.jpg");
			    else if (n.equals ("16.jpg"))
				a [i - 1].setIcon ("32.jpg");
			    else if (n.equals ("32.jpg"))
				a [i - 1].setIcon ("64.jpg");
			    else if (n.equals ("64.jpg"))
				a [i - 1].setIcon ("128.jpg");
			    else if (n.equals ("128.jpg"))
				a [i - 1].setIcon ("256.jpg");
			    else if (n.equals ("256.jpg"))
				a [i - 1].setIcon ("512.jpg");
			    else
				a [i - 1].setIcon ("1024.jpg");


			}
		    }
		}
	    }



	    else if (e.getActionCOmman ().equals ("l"))
	    {
		for (int i = 0 ; i < 15 ; i++)
		{

		    if ((a [i] == a [i + 1] && (i != 3 || i != 7 || i != 11))
		    {
			int t = a [i];
			int g = a [i + 1];
			if (t == g)
			{
			    int n = Integer.parseInt (e.getActionCommand ());
			    if (n.equals ("2.jpg"))
				a [i + 1].setIcon ("4.jpg");
			    else if (n.equals ("4.jpg"))
				a [i - 1].setIcon ("8.jpg");
			    else if (n.equals ("8.jpg"))
				a [i + 1].setIcon ("16.jpg");
			    else if (n.equals ("16.jpg"))
				a [i + 1].setIcon ("32.jpg");
			    else if (n.equals ("32.jpg"))
				a [i + 1].setIcon ("64.jpg");
			    else if (n.equals ("64.jpg"))
				a [i + 1].setIcon ("128.jpg");
			    else if (n.equals ("128.jpg"))
				a [i + 1].setIcon ("256.jpg");
			    else if (n.equals ("256.jpg"))
				a [i + 1].setIcon ("512.jpg");
			    else
				a [i + 1].setIcon ("1024.jpg");

			}
		    }
		}
	    }

	}


	protected static ImageIcon createImageIcon (String path)
	{
	    java.net.URL imgURL = Twenty - Fourty - Eight.class.getResource (path);
	    if (imgURL != null)
	    {
		return new ImageIcon (imgURL);
	    }


	    else
	    {
		System.err.println ("Couldn't find file: " + path);
		return null;
	    }
	}
    }


