package hse.se.aaizmaylov.petrinetslibrary.petrinets;

import hse.se.aaizmaylov.petrinetslibrary.petrinets.basic.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ArcTest {
    @Test
    void withChangedInput() {
        Transition t1 = new TransitionImpl("t1");
        Transition t2 = new TransitionImpl("t2");
        Place p1 = Place.withMarks(2, "p2");

        FromTransitionToPlaceArc transitionToPlaceArc = new FromTransitionToPlaceArc(t1, p1, 11);
        Arc<Long, Long, Transition, Place> newArc = transitionToPlaceArc.withChangedInput(t2);

        assertEquals(t2, newArc.getFromEndpoint());
        assertEquals(p1, newArc.getToEndpoint());
        assertEquals(Long.valueOf(11), newArc.weight());
    }

    @Test
    void withChangedOutput() {
        Transition t1 = new TransitionImpl("t1");
        Place p1 = Place.withMarks(2, "p1");
        Place p2 = Place.withMarks(3, "p2");

        FromTransitionToPlaceArc transitionToPlaceArc = new FromTransitionToPlaceArc(t1, p1, 12);
        Arc<Long, Long, Transition, Place> newArc = transitionToPlaceArc.withChangedOutput(p2);

        assertEquals(t1, newArc.getFromEndpoint());
        assertEquals(p2, newArc.getToEndpoint());
        assertEquals(Long.valueOf(12), newArc.weight());
    }
}