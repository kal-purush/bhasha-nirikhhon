package ui;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.ScrollPaneConstants;

import render.AudioUtility;

public class GameMenu extends JPanel {
	protected JPanel eastPanel;
	protected WestPanel wp;
	protected BufferedImage bg ;

	public GameMenu() {
		this.setSize(new Dimension(800, 600));
		//this.setBackground(Color.BLACK);
		this.setLayout(new BorderLayout());
		eastPanel = new JPanel();
		wp = new WestPanel();
		eastPanel.setLayout(new BorderLayout());
		eastPanel.setPreferredSize(new Dimension(400, 600));
		eastPanel.setBackground(Color.WHITE);
		this.add(wp, BorderLayout.WEST);
		this.add(eastPanel, BorderLayout.EAST);
		this.setVisible(true);

	}
	
/*public void paintComponent(Graphics g){
		super.paintComponents(g);
		Graphics2D g2 = (Graphics2D) g;
		//g2.drawImage(bg, null, 0, 0);
		//g2.setColor(Color.BLACK);
		//g2.setBackground(Color.BLACK);
		//g2.setFont(new Font("Arial", Font.BOLD, 30));
		//FontMetrics fm = g.getFontMetrics(new Font("Arial", Font.BOLD, 30));
		//int width = fm.stringWidth("press any key");
		//g2.drawString("press any key", screenWidth/2- width/2,(int) (0.75*screenHeight));

	}*/
}