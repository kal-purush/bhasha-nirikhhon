package monitor;

import java.util.ArrayList;
import monitor.Human;

public class Bathroom {
	/**
	* fila com homens para entrar no banheiro.
	*/
	private ArrayList<Human> queueMale;
	/**
	* fila com mulheres para entrar no banheiro.
	*/
	private ArrayList<Human> queueFemale;
	/**
	*
	*/
	private int capacity;
	/**
	*
	*/
	private int usingNow;
	/**
	*
	*/
	private int usingNow;


	/**
	*
	*/
	Bathroom(int capacity) {
		this.capacity = capacity;
	    this.queueMale = new ArrayList<Human>();
	    this.queueFemale = new ArrayList<Human>();
	}

	/**
	*
	*/
	public synchronized void callNext() {
	    if(this.capacity > this.usingNow && this.time == 'M') {
	      // remove objeto da fila de homens.
	      // chama metodo "useBathroom" do objeto.
	      // incrementa em mais 1 o valor de "usingNow".
	    }
	    if(this.capacity > this.usingNow && this.time == 'F') {
	      // remove objeto da fila de mulheres.
	      // chama metodo "useBathroom" do objeto.
	      // incrementa em mais 1 o valor de "usingNow".
	    }
	}

	/**
	*
	*/
	void changeTime() {
	    if(this.usingNow == 'M') {
	      this.usingNow = 'F';
	    }
	    else {
	      this.usingNow = 'M';
	    }
	}
}