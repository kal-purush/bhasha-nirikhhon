package hse.se.aaizmaylov.petrinetslibrary.petrinets.basic.analysis;

import hse.se.aaizmaylov.petrinetslibrary.petrinets.PetriNet;
import hse.se.aaizmaylov.petrinetslibrary.petrinets.basic.Place;
import hse.se.aaizmaylov.petrinetslibrary.petrinets.basic.Transition;
import lombok.Getter;
import lombok.NonNull;

public final class DefaultReductionInitializationData {
    @Getter
    private PetriNet<Place, Transition> petriNet;

    public DefaultReductionInitializationData(@NonNull PetriNet<Place, Transition> petriNet) {
        this.petriNet = petriNet;
    }
}