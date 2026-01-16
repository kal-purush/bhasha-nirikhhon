package ladies;

import javax.swing.JPanel;

import java.awt.Color;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.ImageIcon;
import javax.swing.JButton;

public class PaneGameTable extends JPanel implements ActionListener {

	final static int BOARDWIDTH = 8;
	final static int MAX_BUTTONS = 64;

	public final static int EMPTY = 0;
	public final static int BLANCAS = 1;
	public final static int NEGRAS = 2;

	public JButtonTaulell[] arrayButtons;
	public JButtonTaulell[][] taulell;
	int num = 0;
	ImageIcon b;
	ImageIcon n;

	int turno = BLANCAS;
	int origen;
	boolean moveDone = false;

	public PaneGameTable() {

		origen = -1;
		setLayout(new GridLayout(8, 8));

		arrayButtons = new JButtonTaulell[MAX_BUTTONS];
		taulell = new JButtonTaulell[BOARDWIDTH][BOARDWIDTH];
		b = new ImageIcon("img/b.png");
		n = new ImageIcon("img/n.png");

		for (int i = 0; i < BOARDWIDTH; i++) {
			for (int j = 0; j < BOARDWIDTH; j++) {

				taulell[i][j] = new JButtonTaulell(num, i, j);
				arrayButtons[num] = taulell[i][j];
				num++;

				if (i % 2 == 0) {
					if (j % 2 == 0) {
						taulell[i][j].setBackground(Color.white);
					} else {
						taulell[i][j].addActionListener(this);
						taulell[i][j].setBackground(Color.black);
						if (i < 3) {
							taulell[i][j].setEstado(NEGRAS);
							taulell[i][j].setIcon(n);
						} else if (i > 4) {
							taulell[i][j].setEstado(BLANCAS);
							taulell[i][j].setIcon(b);
						} else {
							taulell[i][j].setEstado(EMPTY);
						}
					}
				} else {
					if (j % 2 == 1) {
						taulell[i][j].setBackground(Color.white);
					} else {
						taulell[i][j].addActionListener(this);
						taulell[i][j].setBackground(Color.black);
						if (i < 3) {
							taulell[i][j].setEstado(NEGRAS);
							taulell[i][j].setIcon(n);
						} else if (i > 4) {
							taulell[i][j].setEstado(BLANCAS);
							taulell[i][j].setIcon(b);
						} else {
							taulell[i][j].setEstado(EMPTY);
						}
					}
				}
				add(taulell[i][j]);
			}
		}
	}

	@Override
	public void actionPerformed(ActionEvent e) {

		JButtonTaulell button = (JButtonTaulell) e.getSource();
		
		int botonSelectedId = button.getId();
		int estado = button.getEstado();

		if (estado == turno) {
			System.out.println("Id: " + botonSelectedId);
			System.out.println("Estado: " + estado);
			System.out.println("Origen: " + origen);
			button.setBackground(Color.BLUE);
			if (origen != -1)
				arrayButtons[origen].setBackground(Color.BLACK);
			/*
			 * //Comprovoar si estas obligado a matar
			 * if(obligadoAMatar&&laFichaSeleccionadaNoEsLaCorrecta){origen=0;}
			 * else{ cambiar imagen boton a seleccionado deseleccionar otros }
			 */
			
			if (botonSelectedId == origen && moveDone == true) {
				button.setBackground(Color.BLACK);
				if (turno == BLANCAS){
					turno = NEGRAS;
				}else{
					turno = BLANCAS;
				}
				origen = -1;
				moveDone= false;
			}else{
				origen = botonSelectedId;
			}

			

		} else if (estado == PaneGameTable.EMPTY && origen != -1) {

			arrayButtons[origen].setEstado(EMPTY);
			arrayButtons[origen].setIcon(null);
			arrayButtons[origen].setBackground(Color.BLACK);
			if (turno == BLANCAS) {
				button.setEstado(BLANCAS);
				button.setIcon(b);
			} else {
				button.setEstado(NEGRAS);
				button.setIcon(n);
			}
			button.setBackground(Color.BLUE);
			System.out.println("Movimiento");

			origen=botonSelectedId;
			moveDone = true;

		} else {
			System.out.println("Estado: " + estado);
			System.out.println("Origen: " + origen);
		}

		/*
		 * movimiento=comprovarMovimeiento(origen, botonSelected); //0 si se
		 * puede mover, 1 no se puede mover, else el ID que mata
		 * if(movimiento==0){ //moverse sin matar
		 * arrayButtons[origen].setEstado(0);
		 * arrayButtons[botonSelected].setEstado(turno); } else
		 * if(movimiento==-1){} //no te puedes mover else{ //el valor movimiento
		 * es el ID de la ficha que se elimina
		 * arrayButtons[origen].setEstado(0);
		 * arrayButtons[movimiento].setEstado(0);
		 * arrayButtons[botonSelected].setEstado(turno); } origen=0; }
		 * 
		 * comprovarMovimiento(int origen, int botonSelected){ int direccion;
		 * if(turno==1){ direccion=-1; } else{ direccion=1; }
		 * 
		 * if(origen%8==0){ if(botonSelected-origen==7*direccion){return 0;}
		 * if(botonSelected-origen==14*direccion){ return
		 * comprovarEliminacion(origen,destino); } } else
		 * if(origen%8==1||origen=1){
		 * if(botonSelected-origen==9*direccion){return 0;}
		 * if(botonSelected-origen==18*direccion){ return
		 * comprovarEliminacion(origen,destino); } } else{
		 * if(botonSelected-origen==7*direccion||botonSelected-origen==9*
		 * direccion){return 0;}
		 * if(botonSelected-origen==14*direccion||botonSelected-origen==18*
		 * direccion){ return comprovarEliminacion(origen,destino); } } }
		 * 
		 * comprovarEliminacion(int origen, int destino){ int
		 * eliminada=origen+((destino-origen)/2);
		 * if(arrayButtons[eliminada].getEstado()!turno&&estado!=0){ return
		 * eliminada; } return -1; }
		 * 
		 * 
		 */

	}

}