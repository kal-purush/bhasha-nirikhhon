package Tambola;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Card {
	Ticket[] card;
	
	public Card(int tickets) {
		card = new Ticket[tickets];
		Set<Ticket> cardSet = new HashSet<Ticket>();
		while (cardSet.size() < tickets) {
			cardSet.add(new Ticket());
		}
		cardSet.toArray(card);
	}

	public String get(String indent) {
		String c = "";
		int count = 1;
		for (Ticket t : card) {
			c += indent + "Ticket " + count + ": \n" + t.get("");
			count++;
		}
		return c;
	}
	
	public boolean valid() {
		for (Ticket t: card) {
			if (!t.valid()) return false;
		}
		return true;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(card);
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof Card)) {
			return false;
		}
		Card other = (Card) obj;
		if (!Arrays.equals(card, other.card)) {
			return false;
		}
		return true;
	}
}