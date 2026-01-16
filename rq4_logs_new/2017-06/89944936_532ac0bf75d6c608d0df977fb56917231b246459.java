package pkg09.pkgtry.with.resources;

import java.util.Collection;
import java.util.LinkedList;

public class Hinnasto {
    private Collection<Tuoteryhmä> tuoteryhmät;

    public Hinnasto() {
    }

    public Collection<Tuoteryhmä> getTuoteryhmät() {
        return tuoteryhmät;
    }

    public void setTuoteryhmät(Collection<Tuoteryhmä> tuoteryhmät) {
        this.tuoteryhmät = tuoteryhmät;
    }
    
    public void add(Tuoteryhmä tr) {
        if (tuoteryhmät == null) {
            tuoteryhmät = new LinkedList<>();
        }
        tuoteryhmät.add(tr);
    }
}