package blog;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;


import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextArea;
import javax.swing.JTextField;


import javax.swing.text.AbstractDocument;
import javax.swing.text.AttributeSet;
import javax.swing.text.BadLocationException;
import javax.swing.text.DocumentFilter;

public class BlogGUI   {
	
	private final int size = 800;
	
	private JFrame mainFrame;
	private JTextArea postTextArea;
	private JTextField postContent;
	private JButton refresh;
	private JButton post;
	private JPanel subWindow;
	private JPanel buttonWin;
	private JLabel instructions;
	private final int maxCharacters = 140;
	
	private int wordCount = 140;
	

	public BlogGUI() {
		super();
		// TODO Auto-generated constructor stub
	}

	public void setWindow(){
		

		
		mainFrame = new JFrame("Micro Blog Demo");
		mainFrame.setSize(size, size);
		mainFrame.setLayout(new GridLayout(2,1));
		mainFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		
		subWindow = new JPanel();
		subWindow.setLayout(new BorderLayout());
		subWindow.setPreferredSize(new Dimension(size/2, size/2));
		
		subWindow.setBorder(BorderFactory.createCompoundBorder(BorderFactory.createRaisedBevelBorder(), BorderFactory.createLoweredBevelBorder()));
		
		
		instructions = new JLabel("You can still input "+wordCount+" Character(s)");
		subWindow.add(instructions, BorderLayout.NORTH);
		
		postTextArea = new JTextArea(5,20);
		postTextArea.setBackground(new Color(255,250,205));
		postTextArea.setWrapStyleWord(true);
		postTextArea.setLineWrap(true);
		((AbstractDocument) postTextArea.getDocument()).setDocumentFilter(new DocumentFilter(){

			@Override
			public void insertString(FilterBypass fb, int offset,
					String string, AttributeSet attr)
					throws BadLocationException {
				// TODO Auto-generated method stub
				
				if ((fb.getDocument().getLength() + string.length()) <= maxCharacters){
					
					instructions.setText("You can still input "+(maxCharacters-((fb.getDocument().getLength() + string.length())))+" Character(s)");
					super.insertString(fb, offset, string, attr);
					
				}
			}

			@Override
			public void replace(FilterBypass fb, int offset, int length,
					String text, AttributeSet attrs)
					throws BadLocationException {
				// TODO Auto-generated method stub
				
				if ((fb.getDocument().getLength() + text.length()) <= maxCharacters){
					instructions.setText("You can still input "+(maxCharacters-((fb.getDocument().getLength() + text.length())))+" Character(s)");
					super.replace(fb, offset, length, text, attrs);
					
				}
			}

			@Override
			public void remove(FilterBypass fb, int offset, int length)
					throws BadLocationException {
				// TODO Auto-generated method stub
				
				super.remove(fb, offset, length);
				instructions.setText("You can still input "+(maxCharacters-fb.getDocument().getLength())+" Character(s)");
			}
			
		});

	

		subWindow.add(postTextArea, BorderLayout.CENTER);
		
		refresh = new JButton("refresh");
		refresh.setBackground(new Color(31,190,214));
		refresh.addActionListener(new ActionListener(){

			@Override
			public void actionPerformed(ActionEvent e) {
				// TODO Auto-generated method stub
				postContent.setText("You click REFRESH!");
				
			}
			
		});
		
		post = new JButton("post");
		post.setBackground(new Color(255,228,225));
		post.addActionListener(new ActionListener(){

			@Override
			public void actionPerformed(ActionEvent e) {
				// TODO Auto-generated method stub
				postContent.setText("You click POST!");
			}
			
		});
		
		buttonWin = new JPanel();
		buttonWin.setLayout(new GridLayout(1,2));
		
		buttonWin.add(refresh);
		buttonWin.add(post);
		subWindow.add(buttonWin, BorderLayout.SOUTH);
		
		mainFrame.add(subWindow);
		
		postContent = new JTextField(20);
		post.setBackground(Color.LIGHT_GRAY);
		postContent.setHorizontalAlignment(JTextField.CENTER);
		postContent.setText("Here is put my post!!!");
		postContent.setBorder(BorderFactory.createCompoundBorder(BorderFactory.createRaisedBevelBorder(), BorderFactory.createLoweredBevelBorder()));
		mainFrame.add(postContent);
		
		
		mainFrame.setVisible(true);
	}
	
	public static void main(String[] args){
		BlogGUI blogGUi = new BlogGUI();
		blogGUi.setWindow();
	}
	

	



	
	
}