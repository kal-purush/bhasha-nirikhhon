package hse.se.aaizmaylov.petrinetslibrary.petrinets.basic.reductions;

import hse.se.aaizmaylov.petrinetslibrary.petrinets.Arc;
import hse.se.aaizmaylov.petrinetslibrary.petrinets.analysis.Reduction;
import hse.se.aaizmaylov.petrinetslibrary.petrinets.analysis.TransformCallback;
import hse.se.aaizmaylov.petrinetslibrary.petrinets.basic.Place;
import hse.se.aaizmaylov.petrinetslibrary.petrinets.basic.Transition;
import hse.se.aaizmaylov.petrinetslibrary.petrinets.basic.TransitionImpl;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.stream.Collectors;

public class PostAgglomerationOfTransitions implements Reduction<Place, Transition> {

    private final static Logger LOGGER = Logger.getLogger(PostAgglomerationOfTransitions.class);

    @Override
    public boolean reduceFrom(@NonNull Place place, @NonNull TransformCallback<Place, Transition> callback) {
        Pair<Set<Transition>, Set<Transition>> postPreTransitions = findApplicableTransitions(place);

        if (postPreTransitions == null)
            return false;

        LOGGER.debug("Post agglomeration of transitions!!");

        Set<Transition> postTransitions = postPreTransitions.getLeft();
        Set<Transition> preTransitions = postPreTransitions.getRight();

        Map<Transition, List<Arc<Long, Long, Place, Transition>>> postInputs = new HashMap<>();
        Map<Transition, List<Arc<Long, Long, Transition, Place>>> postOutputs = new HashMap<>();
        Map<Transition, List<Arc<Long, Long, Transition, Place>>> preOutputs = new HashMap<>();

        for (Transition post : postTransitions) {
            List<Arc<Long, Long, Place, Transition>> inputs = new ArrayList<>(post.getInputs());
            inputs.forEach(arc -> arc.getFromEndpoint().removeOutput(arc));

            postInputs.put(post, inputs);

            List<Arc<Long, Long, Transition, Place>> outputs = new ArrayList<>(post.getOutputs());
            outputs.forEach(arc -> arc.getToEndpoint().removeInput(arc));

            postOutputs.put(post, outputs.stream()
                    .filter(arc -> !arc.getToEndpoint().equals(place))
                    .collect(Collectors.toList())
            );
        }

        for (Transition pre : preTransitions) {
            List<Arc<Long, Long, Transition, Place>> outputs = new ArrayList<>(pre.getOutputs());
            outputs.forEach(arc -> arc.getToEndpoint().removeInput(arc));

            preOutputs.put(pre, outputs);
        }

        callback.onDeleteTarget(place);
        preTransitions.forEach(callback::onDeleteNeighbour);
        postTransitions.forEach(callback::onDeleteNeighbour);

        mergeTransitions(postTransitions, preTransitions, postInputs, postOutputs, preOutputs, callback);

        return true;
    }

    private void mergeTransitions(@NotNull Set<Transition> postTransitions, @NotNull Set<Transition> preTransitions,
                                  @NotNull Map<Transition, List<Arc<Long, Long, Place, Transition>>> postInputs,
                                  @NotNull Map<Transition, List<Arc<Long, Long, Transition, Place>>> postOutputs,
                                  @NotNull Map<Transition, List<Arc<Long, Long, Transition, Place>>> preOutputs,
                                  @NotNull TransformCallback<Place, Transition> callback) {

        for (Transition post : postTransitions) {
            for (Transition pre : preTransitions) {
                Transition t = new TransitionImpl(post.label() + "." + pre.label());
                callback.onAddNeighbour(t);

                postInputs.get(post).forEach(input -> t.addInput(input.withChangedOutput(t)));
                postOutputs.get(post).forEach(output -> t.addOutput(output.withChangedInput(t)));
                preOutputs.get(pre).forEach(output -> t.addOutput(output.withChangedInput(t)));
            }
        }
    }

    @Nullable
    private Pair<Set<Transition>, Set<Transition>> findApplicableTransitions(@NotNull Place place) {
        if (place.getMarks() != 0)
            return null;

        Set<Transition> postTransitions = place.getInputs()
                .stream()
                .map(Arc::getFromEndpoint)
                .collect(Collectors.toSet());

        Set<Transition> preTransitions = place.getOutputs()
                .stream()
                .map(Arc::getToEndpoint)
                .collect(Collectors.toSet());

        // not disjoint
        if (postTransitions.stream().anyMatch(preTransitions::contains))
            return null;

        // have internal inputs
        if (preTransitions.stream().anyMatch(t -> t.getInputs().size() != 1))
            return null;

        // haven't outputs
        if (preTransitions.stream().noneMatch(t -> t.getOutputs().size() > 0))
            return null;

        return ImmutablePair.of(postTransitions, preTransitions);
    }
}