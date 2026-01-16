import javax.swing.*;
import java.awt.event.*;
import java.awt.*;
public class Code10{
	public static void main(String[] args){
		Mouselistner ml=new Mouselistner();
	}
}
class Mouselistner extends JFrame{
	public Mouselistner(){
		addMouseListener(new MouseAdapter(){
			public void mousePressed(MouseEvent e){
				System.out.println(e.getX()+","+e.getY());
			}
		});
		setLayout(new FlowLayout());
		setVisible(true);
		setSize(400,400);
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
	}
}