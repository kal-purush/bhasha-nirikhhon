package fr.univ.tlse2.sfr.communication;
/**
 * <b>Classe Position permettant de donner les coordonnées d'un robot sur une carte</b>
 * <p>Une position est caractérisée par :
 * <ul>
 * <li>Une coordonnée en abscisse x </li>
 * <li>Une coordonnée en ordonnée y</li>
 * </ul>
 * </p>
 * @author Nicolas
 */

public class Position {

	/**
	 * Coordonnée x de la position
	 * 
	 * @see Position#Position(double, double)
	 * @see Position#Position(Position)
	 * @see Position#get_x()
	 */
	private double x;
	
	/**
	 * Coordonnée y de la position
	 * 
	 * @see Position#Position(double, double)
	 * @see Position#Position(Position)
	 * @see Position#get_y()
	 */
	private double y;
	
	/**
	 * Constructeur de Position sans paramètres
	 * 
	 * <p>A la construction d'une position sans paramètres d'entrée, les coordonnées en x et en y sont à 0</p>
	 * 
	 * @see Position#x
	 * @see Position#y
	 */
	public Position(){
		this.x = 0.0;
		this.y = 0.0;
	}
	
	/**
	 * Constructeur de Position avec une Position en paramètre d'entrée
	 * <p>A la construction d'une position avec une position en paramètre d'entrée, les nouvelles coordonnées x et y sont recopiées de la position en paramètre d'entrée</p>
	 * @param pos
	 * 			Une position déjà existante
	 */
	public Position(Position pos){
		this.x = pos.get_x();
		this.y = pos.get_y();
	}
	
	/**
	 * Constructeur de Position avec une abscisse et une ordonnée en paramètre d'entrée
	 * <p>Construction d'une position avec une abscisse et une ordonnée </p>
	 *
	 * @param abscisse
	 * 			Une coordonnée x
	 * @param ordonnee
	 * 			Une coordonnée y
	 * @see Position#x
	 */
	public Position(double abscisse, double ordonnee){
		this.x = abscisse;
		this.y = ordonnee;
	}

	public double get_x() {
		return x;
	}

	public void set_x(double x) {
		this.x = x;
	}

	public double get_y() {
		return y;
	}

	public void set_y(double y) {
		this.y = y;
	}
}