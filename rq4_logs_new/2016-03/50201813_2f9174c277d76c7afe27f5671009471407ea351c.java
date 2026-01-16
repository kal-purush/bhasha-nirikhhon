package net.sf.memoranda.ui;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Point;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.KeyListener;
import java.awt.event.KeyEvent;
import java.awt.Font;
import java.awt.Insets;
import java.io.File;

import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

/**
 * PSP2Example creates a filled out version of
 * the PSP time recording log so that the user
 * can seean example of what it is supposed to look like.
 * 
 * @author Casey Vanderham
 * @version 3/18/2016
 */

public class PSPTimeRecording extends JPanel {
	
    //Initializing the labels
	JLabel title = new JLabel("PSP0 Time Recording Log");
	JLabel date = new JLabel("Date");
	JLabel start = new JLabel("Start");
	JLabel stop = new JLabel("Stop");
	JLabel phase = new JLabel("Phase");
	JLabel comments = new JLabel("Comments");
	
	//Initializing the date text fields
	JTextField date1 = new JTextField(10);
	JTextField date2 = new JTextField(10);
	JTextField date3 = new JTextField(10);
	JTextField date4 = new JTextField(10);
	JTextField date5 = new JTextField(10);
	JTextField date6 = new JTextField(10);
	JTextField date7 = new JTextField(10);
	JTextField date8 = new JTextField(10);
	JTextField date9 = new JTextField(10);
	JTextField date10 = new JTextField(10);
	
	//Initizlizing the start text fields
	JTextField start1 = new JTextField(10);
	JTextField start2 = new JTextField(10);
	JTextField start3 = new JTextField(10);
	JTextField start4 = new JTextField(10);
	JTextField start5 = new JTextField(10);
	JTextField start6 = new JTextField(10);
	JTextField start7 = new JTextField(10);
	JTextField start8 = new JTextField(10);
	JTextField start9 = new JTextField(10);
	JTextField start10 = new JTextField(10);
	
	//Initialzing the stop text fields
	JTextField stop1 = new JTextField(10);
	JTextField stop2 = new JTextField(10);
	JTextField stop3 = new JTextField(10);
	JTextField stop4 = new JTextField(10);
	JTextField stop5 = new JTextField(10);
	JTextField stop6 = new JTextField(10);
	JTextField stop7 = new JTextField(10);
	JTextField stop8 = new JTextField(10);
	JTextField stop9 = new JTextField(10);
	JTextField stop10 = new JTextField(10);
	
	//Initializing the phase text fields
	JTextField phase1 = new JTextField(10);
	JTextField phase2 = new JTextField(10);
	JTextField phase3 = new JTextField(10);
	JTextField phase4 = new JTextField(10);
	JTextField phase5 = new JTextField(10);
	JTextField phase6 = new JTextField(10);
	JTextField phase7 = new JTextField(10);
	JTextField phase8 = new JTextField(10);
	JTextField phase9 = new JTextField(10);
	JTextField phase10 = new JTextField(10);
	
	//Initialzing the comments text fields
	JTextField comments1 = new JTextField(25);
	JTextField comments2 = new JTextField(25);
	JTextField comments3 = new JTextField(25);
	JTextField comments4 = new JTextField(25);
	JTextField comments5 = new JTextField(25);
	JTextField comments6 = new JTextField(25);
	JTextField comments7 = new JTextField(25);
	JTextField comments8 = new JTextField(25);
	JTextField comments9 = new JTextField(25);
	JTextField comments10 = new JTextField(25);
	
    public PSPTimeRecording() {
    	
        try {
            jbInit();
                        
        }
        catch (Exception ex) {
           new ExceptionDialog(ex);
        }
    }
    
    
    void jbInit() throws Exception {
        this.setLayout(null);
        Insets insets = this.getInsets();
        
        //Title label
        Font titleFont = new Font("Courier", Font.BOLD, 35);
        title.setFont(titleFont);
        Dimension titleSize = title.getPreferredSize();
        title.setBounds(25 + insets.left, 15 + insets.top, titleSize.width, titleSize.height);
        
        //Date label
        Font dateFont = new Font("Courier", Font.BOLD, 20);
        date.setFont(dateFont);
        Dimension dateSize = date.getPreferredSize();
        date.setBounds(60 + insets.left, 80 + insets.top, dateSize.width, dateSize.height);
        
        //Start label
        start.setFont(dateFont);
        Dimension startSize = start.getPreferredSize();
        start.setBounds(100 + insets.left + dateSize.width, 80 + insets.top, startSize.width, startSize.height);
        
        //Stop label
        stop.setFont(dateFont);
        Dimension stopSize = stop.getPreferredSize();
        stop.setBounds(125 + insets.left + dateSize.width + startSize.width, 80 + insets.top, stopSize.width, stopSize.height);
        
        //Phase label
        phase.setFont(dateFont);
        Dimension phaseSize = phase.getPreferredSize();
        phase.setBounds(165 + insets.left + dateSize.width + startSize.width + stopSize.width, 80 + insets.top, phaseSize.width, phaseSize.height);
        
        //Comments label
        comments.setFont(dateFont);
        Dimension commentsSize = comments.getPreferredSize();
        comments.setBounds(190 + insets.left + dateSize.width + startSize.width + stopSize.width + phaseSize.width, 80 + insets.top, commentsSize.width, commentsSize.height);
        
        //Adding the labels
        this.add(title);
        this.add(date);
        this.add(start);
        this.add(stop);
        this.add(phase);
        this.add(comments);
        
    }
}