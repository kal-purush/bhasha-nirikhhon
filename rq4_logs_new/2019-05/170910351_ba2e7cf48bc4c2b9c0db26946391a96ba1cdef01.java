package hse.se.aaizmaylov.petrinetslibrary.petrinets.analysis;

import lombok.NonNull;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import static hse.se.aaizmaylov.petrinetslibrary.petrinets.analysis.EmptyCallback.EMPTY;

public interface TransformCallback<TTarget, TNeighbour> {
    void onDeleteTarget(TTarget target);

    void onDeleteNeighbour(TNeighbour neighbour);

    void onAddTarget(TTarget target);

    void onAddNeighbour(TNeighbour neighbour);

    @NotNull
    @Contract(value = "_ -> new", pure = true)
    static <TTarget, TNeighbour> TransformCallback<TNeighbour, TTarget> invertedAdapter(
            @NonNull TransformCallback<TTarget, TNeighbour> callback) {

        return new TransformCallback<TNeighbour, TTarget>() {
            @Override
            public void onDeleteTarget(TNeighbour neighbour) {
                callback.onDeleteNeighbour(neighbour);
            }

            @Override
            public void onDeleteNeighbour(TTarget target) {
                callback.onDeleteTarget(target);
            }

            @Override
            public void onAddTarget(TNeighbour neighbour) {
                callback.onAddNeighbour(neighbour);
            }

            @Override
            public void onAddNeighbour(TTarget target) {
                callback.onAddTarget(target);
            }
        };
    }

    @Contract(pure = true)
    @SuppressWarnings("unchecked")
    static <TTarget, TNeighbour> TransformCallback<TTarget, TNeighbour> empty() {
        return EMPTY;
    }
}