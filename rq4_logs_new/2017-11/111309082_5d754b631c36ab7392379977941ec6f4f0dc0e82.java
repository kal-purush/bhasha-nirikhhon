package dt2.controlador;

import java.awt.CardLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import dt2.vista.InterfazdePaneles;


public class ControlBotonMenu implements ActionListener {
	InterfazdePaneles paneles;
	
	public ControlBotonMenu(InterfazdePaneles paneles) {
		this.paneles=paneles;		
	}
	@Override
	public void actionPerformed(ActionEvent e) {
		String evento= e.getActionCommand();
		
		if(evento.equals("Descripci�n")) {
			CardLayout c1=(CardLayout)(paneles.getLayout());
			
			c1.show(paneles, "descripcion");
			System.out.println("Descripci�n");
		}
		if(evento.equals("Balance de L�nea")) {
			CardLayout c1=(CardLayout) (paneles.getLayout());
			c1.show(paneles, "balance");
			String nombre=paneles.getName();
			System.out.println("Balance");
			System.out.println(nombre);
		}
		if(evento.equals("Flujo de Caja")) {
			CardLayout c1=(CardLayout) (paneles.getLayout());
			c1.show(paneles, "flujo");
			System.out.println("ultimo");}
		if(evento.equals("Gr�ficas")) {
			CardLayout c1=(CardLayout) paneles.getLayout();
			c1.show(paneles, "graficos");
		}
		
		
		
	
	}
		
	

}