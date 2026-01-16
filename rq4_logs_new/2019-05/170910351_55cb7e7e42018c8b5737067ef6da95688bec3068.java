package hse.se.aaizmaylov.petrinetslibrary.petrinets.basic.reductions;

import hse.se.aaizmaylov.petrinetslibrary.petrinets.Arc;
import hse.se.aaizmaylov.petrinetslibrary.petrinets.analysis.DeleteVertexCallback;
import hse.se.aaizmaylov.petrinetslibrary.petrinets.analysis.Reduction;
import hse.se.aaizmaylov.petrinetslibrary.petrinets.basic.FromPlaceToTransitionArc;
import hse.se.aaizmaylov.petrinetslibrary.petrinets.basic.Place;
import hse.se.aaizmaylov.petrinetslibrary.petrinets.basic.Transition;
import hse.se.aaizmaylov.petrinetslibrary.utils.CollectionsUtils;
import lombok.NonNull;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class PreAgglomerationOfTransition implements Reduction<Place, Transition> {

    @Override
    public boolean reduceFrom(@NonNull Place place, @NonNull DeleteVertexCallback<Place, Transition> callback) {
        List<Arc<Long, Long, Place, Transition>> prePlacesArcs = checkAndGetAllArcsToPrePlaces(place);

        if (prePlacesArcs.isEmpty())
            return false;

        List<Transition> postTransitions = getAllPostTransitions(place);

        final Transition h = CollectionsUtils.first(place.getInputs()).getFromEndpoint();
        callback.onDeleteTarget(place);
        callback.onDeleteNeighbour(h);

        for (Arc<Long, Long, Place, Transition> prePlacesArc : prePlacesArcs) {
            for (Transition postTransition : postTransitions) {
                postTransition.addInput(new FromPlaceToTransitionArc(
                        prePlacesArc.getFromEndpoint(),
                        postTransition,
                        prePlacesArc.weight()));
            }
        }

        new ArrayList<>(h.getInputs()).forEach(arc -> arc.getFromEndpoint().removeOutput(arc));

        return true;
    }

    @NotNull
    private List<Transition> getAllPostTransitions(@NotNull Place place) {
        return place.getOutputs()
                .stream()
                .map(Arc::getToEndpoint)
                .collect(Collectors.toList());
    }

    @NotNull
    private List<Arc<Long, Long, Place, Transition>> checkAndGetAllArcsToPrePlaces(@NotNull Place place) {
        if (place.getMarks() != 0 || place.getInputs().size() != 1)
            return Collections.emptyList();

        Transition h = CollectionsUtils.first(place.getInputs()).getFromEndpoint();

        if (h.getOutputs().size() != 1)
            return Collections.emptyList();

        List<Arc<Long, Long, Place, Transition>> prePlaces = new ArrayList<>(h.getInputs());

        if (!prePlaces
                .stream()
                .map(Arc::getFromEndpoint)
                .allMatch(p -> p.getOutputs().size() == 1)) {
            return Collections.emptyList();
        }

        if (prePlaces.size() == 0) {
            return Collections.emptyList();
        }

        if (prePlaces.stream().anyMatch(arc -> arc.getFromEndpoint() == place)) {
            return Collections.emptyList();
        }

        return prePlaces;
    }
}