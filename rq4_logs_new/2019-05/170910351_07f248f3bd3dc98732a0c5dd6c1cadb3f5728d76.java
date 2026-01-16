package hse.se.aaizmaylov.petrinetslibrary.petrinets.analysis;

import hse.se.aaizmaylov.petrinetslibrary.petrinets.LabeledVertex;
import lombok.NonNull;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.stream.Collectors;

public class ReductionHistory {
    private Map<LabeledVertex, List<String>> history;

    @SafeVarargs
    public ReductionHistory(@NonNull List<LabeledVertex>... verticesList) {
        history = Arrays.stream(verticesList)
                .flatMap(Collection::stream)
                .collect(Collectors.toMap(v -> v,
                v -> new ArrayList<>(Collections.singleton(v.label()))));
    }

    public void delete(@NonNull LabeledVertex vertex) {
        history.remove(vertex);
    }

    public void merge(@NonNull LabeledVertex to, @NonNull LabeledVertex... mergedVertices) {
        List<String> labels = history.computeIfAbsent(to, v -> new ArrayList<>());

        for (LabeledVertex vertexToMerge : mergedVertices) {
            List<String> labelsToAdd = history.get(Objects.requireNonNull(vertexToMerge));

            labels.addAll(Objects.requireNonNull(labelsToAdd));

            history.remove(vertexToMerge);
        }
    }

    @NotNull
    public List<String> get(@NonNull LabeledVertex vertex) {
        return Collections.unmodifiableList(Objects.requireNonNull(history.get(vertex)));
    }

    public boolean contains(@NonNull LabeledVertex vertex) {
        return history.containsKey(vertex);
    }
}