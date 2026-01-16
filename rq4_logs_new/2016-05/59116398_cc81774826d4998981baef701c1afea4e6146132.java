import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * Dijkstra's Algorithm, based on the version in: http://rosettacode.org/wiki/Dijkstra%27s_algorithm#Java
 *
 * Classe rapprensentante il grafo necessario per l'esecuzione dell'algoritmo di
 * Dijkstra per il percorso con costo minore.
 */
public class Dijkstra {
	private final HashMap<String, Vertice> grafo;
	private String partenza;

	/**
	 * Costruttore di default. Necessita tutti i tratti necessari alla
	 * composizione del grafo.
	 * <hr>
	 * Per ogni punto dei tratti crea inserisce una stringa sul grafo fittizio,
	 * e successivamente crea un Vertice per ogni colleagmenti che il punto
	 * possiede con altri.
	 * 
	 * @param tratti
	 *        Array dei tratti del grafo
	 */
	public Dijkstra(Tratto[] tratti) {
		grafo = new HashMap<String, Vertice>();

		// Inserisce tutti i punti nelle mappa
		for (Tratto t : tratti) {
			if (!getGrafo().containsKey(t.getPunto1())) getGrafo().put(
					t.getPunto1(), new Vertice(t.getPunto1()));
			if (!getGrafo().containsKey(t.getPunto2())) getGrafo().put(
					t.getPunto2(), new Vertice(t.getPunto2()));
		}

		// Aggiunge a tutti i punti il relativo costo di collegamento
		for (Tratto t : tratti) {
			getGrafo().get(t.getPunto1()).getVicini().put(
					getGrafo().get(t.getPunto2()), t.getCosto());
		}
	}

	/**
	 * Costruttore di un grafo bidirezionale.
	 * 
	 * @param tratti
	 *        Array dei tratti del grafo
	 * @param bidirezionale
	 *        Flag rappresentante se si desidera la bidirezionalità
	 * @see {@link #Dijkstra}.
	 */
	public Dijkstra(Tratto[] tratti, boolean bidirezionale) {
		this(tratti);
		if (bidirezionale) {
			for (Tratto t : tratti) {
				getGrafo().get(t.getPunto2()).getVicini().put(
						getGrafo().get(t.getPunto1()), t.getCosto());
			}
		}
	}

	/**
	 * Prepara il campo per l'esecuzione dell'algoritmo di Dijkstra.
	 * <hr>
	 * Controlla se i punti inseriti esistono, poi azzera <b>costo</b> e
	 * <b>precedente</b> di tutti i Vertici, inserendoli in una lista ordinata
	 * per <b>costo</b> minore.<br>
	 * Esegue l'algoritmo sulla lista ordinata.
	 * 
	 * @param partenza
	 *        Nome del vertice (punto) di partenza
	 * @param arrivo
	 *        Nome del vertice (punto) di partenza
	 * @return Percorso migliore
	 */
	public String dijkstra(String partenza, String arrivo) {
		if (!getGrafo().containsKey(partenza)
				|| !getGrafo().containsKey(arrivo)) return null;
		if (getPartenza() == null || !getPartenza().equals(partenza)) {
			setPartenza(partenza);
			Vertice inizio = getGrafo().get(partenza);
			ArrayList<Vertice> lista = new ArrayList<Vertice>();
			for (Vertice vert : getGrafo().values()) {
				if (vert == inizio) {
					vert.setCosto(0);
					vert.setPrecedente(inizio);
				}
				else {
					vert.setCosto(Integer.MAX_VALUE);
					vert.setPrecedente(null);
				}
				lista.add(vert);
			}
			dijkstra(lista);
		}
		return percorso(arrivo);
	}

	/**
	 * Prepara il campo per l'esecuzione dell'algoritmo di Dijkstra.
	 * <hr>
	 * Controlla se i punti inseriti esistono, poi azzera <b>costo</b> e
	 * <b>precedente</b> di tutti i Vertici, inserendoli in una lista ordinata
	 * per <b>costo</b> minore.<br>
	 * Esegue l'algoritmo sulla lista ordinata.
	 * 
	 * @param partenza
	 *        Nome del vertice (punto) di partenza
	 * @param arrivo
	 *        Nome del vertice (punto) di partenza
	 * @return Percorso migliore
	 */
	public int costoDijkstra(String partenza, String arrivo) {
		if (!getGrafo().containsKey(partenza)
				|| !getGrafo().containsKey(arrivo)) return -1;
		if (getPartenza() == null || !getPartenza().equals(partenza)) {
			setPartenza(partenza);
			Vertice inizio = getGrafo().get(partenza);
			ArrayList<Vertice> lista = new ArrayList<Vertice>();
			for (Vertice vert : getGrafo().values()) {
				if (vert == inizio) {
					vert.setCosto(0);
					vert.setPrecedente(inizio);
				}
				else {
					vert.setCosto(Integer.MAX_VALUE);
					vert.setPrecedente(null);
				}
				lista.add(vert);
			}
			dijkstra(lista);
		}
		return costo(arrivo);
	}

	/**
	 * Algoritmo di Dijkstra.
	 * <hr>
	 * Finch� la lista inserita non � vuota:<ul>
	 * <li>Prende il primo elemento (quello con costo minore), rimuovendolo
	 * dalla lista<ul>
	 * <li>Se il costo è equivalente al massimo valore intero, blocca
	 * l'esecuzione -> Impossibile andare avanti</li>
	 * </ul></li>
	 * <li>Per ogni collegamento del Vertice <b>considerato</b>, controlla se il
	 * costo di <b>considerato</b> sommato a quello del passaggio tra
	 * <b>considerato</b> e il vicino (Integer della HashMap di <b>vicini</b>
	 * del Vertice) è minore di quello salvato nel Vertice stesso<ul>
	 * <li>Se si, sostituisce il costo, inserisce come precedente le'lemento
	 * <b>considerato</b> e riordina la lista</li>
	 * </ul></li>
	 * </ul>
	 * <hr>
	 * Come risultato l'insieme di Vertici del <b>grafo</b> sar� completo di
	 * informazioni per l'ordinamento.
	 * 
	 * @param lista
	 */
	private void dijkstra(List<Vertice> lista) {
		Vertice considerato;
		while (!lista.isEmpty()) {
			Collections.sort(lista);
			considerato = lista.get(0);
			lista.remove(considerato);
			if (considerato.getCosto() == Integer.MAX_VALUE) break;
			for (Vertice vert : considerato.getVicini().keySet()) {
				if (considerato.getCosto() + considerato.getVicini().get(vert) < vert.getCosto()) {
					vert.setCosto(considerato.getCosto()
							+ considerato.getVicini().get(vert));
					vert.setPrecedente(considerato);
				}
			}
		}
	}

	private String percorso(String arrivo) {
		if (!getGrafo().containsKey(arrivo)) {
			System.err.println("Il grafo non contiene il vertice " + arrivo);
			return null;
		}
		String result = getGrafo().get(arrivo).toString();
		if (result.indexOf("(Destinazione non raggiungibile)") == -1) result +=
				" (in " + getGrafo().get(arrivo).getCosto() + " minuti)";
		return result;
	}

	/**
	 * Restituisce il costo del percorso per arrivare all'arrivo.<hr>
	 * <b>ATTENZIONE: come punto di partenza viene considerato quello
	 * dell'ultima esecuzione dell'agoritmo di Dijkstra.</b>
	 * 
	 * @param arrivo
	 *        Nome del vertice (punto) di arrivo
	 * @return Costo del percorso migliore
	 */
	public int costo(String arrivo) {
		if (!getGrafo().containsKey(arrivo)) {
			System.err.println("Il grafo non contiene il vertice " + arrivo);
			return -1;
		}
		return getGrafo().get(arrivo).getCosto();
	}

	public HashMap<String, Vertice> getGrafo() {
		return this.grafo;
	}

	public String getPartenza() {
		return partenza;
	}

	public void setPartenza(String partenza) {
		this.partenza = partenza;
	}

	/**
	 * Memorizzazione iniziale di tutti i collegamenti tra i punti.
	 * 
	 * @author Thomas Zilio
	 *
	 */
	public static class Tratto {
		private String punto1, punto2;
		private int costo;

		public Tratto(String inizio, String fine, int costo) {
			this.punto1 = inizio;
			this.punto2 = fine;
			this.costo = costo;
		}

		public String getPunto1() {
			return this.punto1;
		}

		public void setPunto1(String punto1) {
			this.punto1 = punto1;
		}

		public String getPunto2() {
			return this.punto2;
		}

		public void setPunto2(String punto2) {
			this.punto2 = punto2;
		}

		public int getCosto() {
			return this.costo;
		}

		public void setCosto(int costo) {
			this.costo = costo;
		}

		@Override
		public String toString() {
			return "Tratto [punto1=" + punto1 + ", punto2=" + punto2
					+ ", costo=" + costo + "]";
		}

	}

	/**
	 * Classe dedita alla memorizzazione di tutti i collegamenti ad un punto.
	 * <hr>
	 * Il punto, indentificato tramite il <b>nome</b>, possiede n <b>vicini</b>
	 * (tutti i collegamenti) con salvato i relativi costi di passaggio.
	 * 
	 * I campi di <b>costo</b> e <b>precedente</b> vengono utilizzati
	 * dall'algoritmo per l'individuazione del percorso minimo.
	 * 
	 * @author Thomas Zilio
	 *
	 */
	public class Vertice implements Comparable<Vertice> {
		private final String nome;
		private int costo = Integer.MAX_VALUE;
		private Vertice precedente;
		private final HashMap<Vertice, Integer> vicini =
				new HashMap<Vertice, Integer>();

		public Vertice(String nome) {
			this.nome = nome;
		}

		public String toString() {
			if (getPrecedente() == null) {
				return getNome() + " (Destinazione non raggiungibile)";
			}
			else {
				StringBuilder result = new StringBuilder(";" + getNome());
				Vertice precedente = getPrecedente();
				while (precedente != null
						&& precedente.getPrecedente() != precedente) {
					result.insert(0, ";" + precedente.getNome());
					precedente = precedente.getPrecedente();
				}
				result.insert(0, precedente.getNome());
				return result.toString();
			}
		}

		public String toStringRicorsivo() {
			if (getPrecedente() == this) {
				return getNome();
			}
			else if (getPrecedente() == null) {
				return getNome() + " (Destinazione non raggiungibile)";
			}
			else {
				return getPrecedente().toString() + "; " + getNome();
			}
		}

		@Override
		public int compareTo(Vertice arg0) {
			return Integer.compare(getCosto(), arg0.getCosto());
		}

		public String getNome() {
			return this.nome;
		}

		public int getCosto() {
			return this.costo;
		}

		public void setCosto(int costo) {
			this.costo = costo;
		}

		public Vertice getPrecedente() {
			return this.precedente;
		}

		public void setPrecedente(Vertice precedente) {
			this.precedente = precedente;
		}

		public HashMap<Vertice, Integer> getVicini() {
			return this.vicini;
		}
	}
}