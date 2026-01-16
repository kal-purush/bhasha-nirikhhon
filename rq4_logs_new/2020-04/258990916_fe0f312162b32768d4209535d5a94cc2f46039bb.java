package it.polito.tdp.ruzzle.model;

import java.util.ArrayList;
import java.util.List;

public class Ricerca {

	public List<Pos> trovaParola(String parola, Board board) {

		for (Pos p : board.getPositions()) {
			if (board.getCellValueProperty(p).get().charAt(0) == parola.charAt(0)) {
				// posso iniziare la ricorsione, è inizio potenziale della parola
				List<Pos> percorso = new ArrayList<Pos>();
				percorso.add(p);
				if (cerca(parola, 1, percorso, board))
					// ho trovato parola
					return percorso;
			}
		}

		return null;
	}

	private boolean cerca(String parola, int livello, List<Pos> percorso, Board board) {
		// caso terminale
		if (livello == parola.length())
			return true;

		Pos ultima = percorso.get(percorso.size() - 1);
		List<Pos> adiacenti = board.getAdjacencies(ultima);
		for (Pos p : adiacenti) {
			if (!percorso.contains(p) && parola.charAt(livello) == board.getCellValueProperty(p).get().charAt(0)) {
				// faccio ricorsione
				percorso.add(p);
				if (cerca(parola, livello + 1, percorso, board))
					return true; // uscita rapida in caso di soluzione
				percorso.remove(percorso.size() - 1);
			}
		}
		return false;
	}

}