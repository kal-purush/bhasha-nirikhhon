package dominio;

import java.io.Serializable;

public abstract class Peleador implements Peleable, Serializable {

	private int salud;
	private int fuerza;
	private int defensa;
	private String nombre;
	private int nivel;

	public int getSalud() {
		return salud;
	}

	public void setSalud(int salud) {
		this.salud = salud;
	}

	public int getFuerza() {
		return fuerza;
	}

	public void setFuerza(int fuerza) {
		this.fuerza = fuerza;
	}

	public int getDefensa() {
		return defensa;
	}

	public void setDefensa(int defensa) {
		this.defensa = defensa;
	}

	public String getNombre() {
		return nombre;
	}

	public void setNombre(String nombre) {
		this.nombre = nombre;
	}

	public int getNivel() {
		return nivel;
	}

	public void setNivel(int nivel) {
		this.nivel = nivel;
	}

	public boolean estaVivo() {
		return salud > 0;
	}

	public void despuesDeTurno() {
	}

	public int otorgarExp() {
		return this.nivel * multiplicadorExperiencia();
	}

	protected abstract int multiplicadorExperiencia();

	/**
	 * Recibir daño evaluando probabilidad de evasión y defensa
	 * 
	 * @param daño
	 *            daño inicial que el atacado sufrirá, puede verse modificado
	 * @return daño sufrido, es equivalente a cuánto disminuyó su salud
	 */
	public int serAtacado(int daño) {
		if (MyRandom.nextDouble() >= probabilidadEvitarDañoEnAtaque()) {
			daño -= defensaAlSerAtacado();
			daño = quitarVidaSegunDaño(daño);
			return daño;
		}
		return 0;
	}

	protected abstract double probabilidadEvitarDañoEnAtaque();

	protected abstract int defensaAlSerAtacado();

	protected int quitarVidaSegunDaño(int daño) {
		if (daño > 0) {
			salud -= daño;
		}
		return daño;
	};

	
	/**
	 * Causar daño al atacado evaluando la probabilidad de golpe crítico y su ataque.
	 * 
	 * @param atacado un objeto que implementa la interfaz Peleable, es aquel a ser atacado
	 * @return        daño causado.
	 */
	public int atacar(Peleable atacado) {
		if (puedoAtacar(atacado.estaVivo())) {
			if (MyRandom.nextDouble() <= probabilidadGolpeCritico()) {
				return atacado.serAtacado(this.golpe_critico());
			} else {
				return atacado.serAtacado(getAtaque());
			}
		}
		return 0;
	}

	protected abstract int golpe_critico();
	
	protected abstract double probabilidadGolpeCritico();

	protected boolean puedoAtacar(boolean atacadoEstaVivo) {
		return true;
	}

}