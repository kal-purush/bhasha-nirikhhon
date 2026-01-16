//Andr� Teixeira

import java.util.ArrayList;
import java.util.concurrent.CyclicBarrier;

public class Main {

	public static void main(String[] args) {

		CyclicBarrier barreira  = new CyclicBarrier(3);
		
		ArrayList<Parque> parques = new ArrayList<Parque>();
		parques.add(new Parque("Parque A"));
		parques.add(new Parque("Parque B"));
		parques.add(new Parque("Parque C"));
		parques.add(new Parque("Parque D"));
		parques.add(new Parque("Parque E"));

		new Turma("Turma 1", 5000, parques, barreira).start();
		new Turma("Turma 2", 2000, parques, barreira).start();
		new Turma("Turma 3", 3000, parques, barreira).start();

	}
}