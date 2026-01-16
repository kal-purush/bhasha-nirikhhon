package hse.se.aaizmaylov.terminalclient;

import hse.se.aaizmaylov.petrinetslibrary.petrinets.analysis.Reduction;
import hse.se.aaizmaylov.petrinetslibrary.petrinets.basic.Place;
import hse.se.aaizmaylov.petrinetslibrary.petrinets.basic.Transition;
import hse.se.aaizmaylov.petrinetslibrary.petrinets.basic.reductions.*;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.stream.Collectors;

public class Reductions {
    private final Map<String, Reduction<Place, Transition>> reductionsOnPlaces;
    private final Map<String, Reduction<Transition, Place>> reductionsOnTransition;

    public Reductions() {
        Map<String, Reduction<Place, Transition>> reductionsOnPlaces = new HashMap<>();

        reductionsOnPlaces.put("PreA", new PreAgglomerationOfTransition());
        reductionsOnPlaces.put("PostA", new PostAgglomerationOfTransitions());
        reductionsOnPlaces.put("RedunP", new DeleteRedundantPlace());
        reductionsOnPlaces.put("FusParT", new FusionOfParallelTransitions());
        reductionsOnPlaces.put("FusLoopT", new FusionOfSelfLoopTransitions());

        Map<String, Reduction<Transition, Place>> reductionsOnTransitions = new HashMap<>();
        reductionsOnTransitions.put("FusParP", new FusionOfParallelPlaces());
        reductionsOnTransitions.put("FusLoopP", new FusionOfSelfLoopPlaces());

        this.reductionsOnPlaces = Collections.synchronizedMap(reductionsOnPlaces);
        this.reductionsOnTransition = Collections.synchronizedMap(reductionsOnTransitions);
    }

    public List<String> parseToList(String reductions) {
        return Arrays.stream(reductions.split(","))
                .map(String::trim)
                .collect(Collectors.toList());
    }

    public Pair<List<Reduction<Place, Transition>>, List<Reduction<Transition, Place>>>
    getReductions(List<String> reductions) {

        if (reductions.size() == 0)
            throw new UnknownReductionException("Unexpected number of reductions!");

        List<Reduction<Place, Transition>> onPlaces = new ArrayList<>();
        List<Reduction<Transition, Place>> onTransition = new ArrayList<>();

        for (String reduction : reductions) {
            if (reductionsOnPlaces.containsKey(reduction)) {
                onPlaces.add(reductionsOnPlaces.get(reduction));
            } else if (reductionsOnTransition.containsKey(reduction)) {
                onTransition.add(reductionsOnTransition.get(reduction));
            } else {
                throw new UnknownReductionException("Unknown reduction " + reduction);
            }
        }

        return ImmutablePair.of(onPlaces, onTransition);
    }

    public Pair<List<Reduction<Place, Transition>>, List<Reduction<Transition, Place>>> all() {
        List<String> res = new ArrayList<>(reductionsOnPlaces.keySet());
        res.addAll(reductionsOnTransition.keySet());
        return getReductions(res);
    }
}