package hse.se.aaizmaylov.petrinetslibrary.petrinets.basic.reductions;

import hse.se.aaizmaylov.petrinetslibrary.petrinets.Arc;
import hse.se.aaizmaylov.petrinetslibrary.petrinets.PetriNet;
import hse.se.aaizmaylov.petrinetslibrary.petrinets.analysis.InitializedReduction;
import hse.se.aaizmaylov.petrinetslibrary.petrinets.analysis.TransformCallback;
import hse.se.aaizmaylov.petrinetslibrary.petrinets.basic.Place;
import hse.se.aaizmaylov.petrinetslibrary.petrinets.basic.Transition;
import hse.se.aaizmaylov.petrinetslibrary.utils.math.IntMatrix;
import hse.se.aaizmaylov.petrinetslibrary.utils.math.IntVector;
import lombok.NonNull;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DeleteRedundantPlace implements InitializedReduction<PetriNet<Place, Transition>, Place, Transition> {

    private IntMatrix incidenceMatrix;
    private Map<Place, Integer> placesIndices;
    private Map<Transition, Integer> transitionsIndices;
    private List<IntVector> pInvariants = new ArrayList<>();
    private boolean initialized = false;

    @Override
    public void initialize(@NonNull PetriNet<Place, Transition> petriNet) {
        if (petriNet.getPlaces().size() == 0 || petriNet.getTransitions().size() == 0)
            return;

        getIncidenceMatrix(petriNet);
        eliminationOfGauss(petriNet);

        initialized = true;
    }


    @Override
    public boolean reduceFrom(@NonNull Place place, @NonNull TransformCallback<Place, Transition> callback) {
        if (!initialized)
            return false;

        IntVector invariant = findInvariant(place);
        if (invariant == null)
            return false;

        IntVector marking = getMarking();
        long criteria = marking.dot(invariant);

        if (!transitionsIndices.entrySet()
                .stream()
                .allMatch(entry -> criteria >= invariant.dot(getPre(entry.getKey())))) {

            return false;
        }

        callback.onDeleteTarget(place);

        for (Arc<Long, Long, Place, Transition> arc : new ArrayList<>(place.getOutputs())) {
            arc.getToEndpoint().removeInput(arc);
        }

        for (Arc<Long, Long, Transition, Place> arc : new ArrayList<>(place.getInputs())) {
            arc.getFromEndpoint().removeOutput(arc);
        }

        return true;
    }

    @NotNull
    private IntVector getMarking() {
        IntVector marking = new IntVector(placesIndices.size());

        placesIndices.forEach(((place, index) -> marking.set(index, place.getMarks())));

        return marking;
    }

    private static int sign(long a) {
        return (a == 0) ? 0 : (a < 0) ? -1 : 1;
    }

    @Nullable
    private IntVector findInvariant(@NotNull Place place) {
        int placeIndex = placesIndices.get(place);

        for (IntVector pInvariant : pInvariants) {
            int othersSign = 0;
            boolean inappropriate = false;

            for (int i = 0, size = pInvariant.size(); i < size; i++) {
                if (i == placeIndex)
                    continue;

                if (othersSign != 0 && pInvariant.get(i) != 0 && sign(othersSign) != sign(pInvariant.get(i))) {
                    inappropriate = true;
                    break;
                }

                if (othersSign == 0 && pInvariant.get(i) != 0)
                    othersSign = sign(pInvariant.get(i));
            }

            int placeSign = sign(pInvariant.get(placeIndex));

            if (placeSign == 0 || inappropriate)
                continue;

            if (placeSign < 0 && othersSign >= 0)
                return pInvariant.inverted();
            else if (placeSign > 0 && othersSign <= 0)
                return pInvariant;
        }

        return null;
    }

    private void eliminationOfGauss(@NotNull PetriNet<Place, Transition> petriNet) {
        for (Place place : petriNet.getPlaces()) {
            pInvariants.add(placeVector(place));
        }

        for (Transition transition : petriNet.getTransitions()) {
            int pivotIndex = -1;
            final Integer column = transitionsIndices.get(transition);

            for (int i = 0; i < pInvariants.size(); i++) {

                if (incidenceMatrix.dotWithColumn(pInvariants.get(i), column) != 0) {
                    pivotIndex = i;
                    break;
                }
            }

            if (pivotIndex == -1)
                continue;

            IntVector pivot = pInvariants.get(pivotIndex);
            pInvariants.remove(pivotIndex);

            pInvariants = pInvariants.stream()
                    .map(v ->
                            v.multiply(incidenceMatrix.dotWithColumn(pivot, column))
                            .subtract(pivot.multiply(incidenceMatrix.dotWithColumn(v, column))
                            .normalized())
                    ).collect(Collectors.toList());

        }
    }

    private void getIncidenceMatrix(@NotNull PetriNet<Place, Transition> petriNet) {
        incidenceMatrix = new IntMatrix(petriNet.getPlaces().size(), petriNet.getTransitions().size());

        placesIndices = getIndices(petriNet.getPlaces());
        transitionsIndices = getIndices(petriNet.getTransitions());


        for (Place place : petriNet.getPlaces()) {
            incidenceMatrix.setRows(placesIndices.get(place), getPost(place).subtract(getPre(place)));
        }
    }

    @NotNull
    private IntVector placeVector(@NotNull Place place) {
        IntVector result = new IntVector(placesIndices.size());

        result.set(placesIndices.get(place), 1);

        return result;
    }

    @NotNull
    private IntVector getPre(@NotNull Place place) {
        IntVector result = new IntVector(transitionsIndices.size());

        for (Arc<Long, Long, Place, Transition> output : place.getOutputs()) {
            result.set(transitionsIndices.get(output.getToEndpoint()), output.weight());
        }

        return result;
    }

    @NotNull
    private IntVector getPre(@NotNull Transition transition) {
        IntVector result = new IntVector(placesIndices.size());

        for (Arc<Long, Long, Place, Transition> input : transition.getInputs()) {
            result.set(placesIndices.get(input.getFromEndpoint()), input.weight());
        }

        return result;
    }

    @NotNull
    private IntVector getPost(@NotNull Place place) {
        IntVector result = new IntVector(transitionsIndices.size());

        for (Arc<Long, Long, Transition, Place> input : place.getInputs()) {
            result.set(transitionsIndices.get(input.getFromEndpoint()), input.weight());
        }

        return result;
    }

    @NotNull
    private static <T> Map<T, Integer> getIndices(@NotNull Iterable<? extends T> ts) {
        Map<T, Integer> result = new HashMap<>();
        int id = 0;

        for (T t : ts) {
            result.put(t, id++);
        }

        return result;
    }
}