import java.util.Comparator;

public class BySuitInValueOrder implements Comparator<Card> {
    
    @Override
    public int compare(Card o1, Card o2) {
        //If suits are the same, cmp will equal 0 and will sort by suit.
        //Then will continue to sort among the two card values.
        int cmp = o1.getSuit().compareTo(o2.getSuit());
        if (cmp != 0) {
            return cmp;
        }
        return o1.getValue() - o2.getValue();
    }
    
}