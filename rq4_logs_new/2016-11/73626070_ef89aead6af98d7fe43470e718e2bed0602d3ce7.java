/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mastermind2;

/**
 *
 * @author laurin.agostini
 */
public class CircleJPanel extends javax.swing.JPanel {
    
	private java.awt.Color color;
	
	public CircleJPanel(java.awt.Color color){
		this.color = color;
	}
	
	public void setColor(java.awt.Color color){
		this.color = color;
	}
	
	
    @Override
    protected void paintComponent(java.awt.Graphics g){
    	g.setColor(color);
    	g.fillOval(0, 0, getSize().width, getSize().height);
    	g.setColor(java.awt.Color.BLACK);
    	g.drawOval(0, 0, getSize().width, getSize().height);
    }
    
}