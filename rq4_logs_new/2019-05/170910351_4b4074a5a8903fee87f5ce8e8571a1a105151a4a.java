package hse.se.aaizmaylov.petrinetslibrary.petrinets.basic;

import lombok.NonNull;
import org.jetbrains.annotations.NotNull;

public class Transitions {
    public static void revertFiring(@NonNull Transition transition) {
        if (!canBeReverted(transition))
            return;

        transition.getOutputs()
                .forEach(arc -> arc.getToEndpoint().removeMarks(arc.weight()));
        transition.getInputs()
                .forEach(arc -> arc.getFromEndpoint().addMarks(arc.weight()));
    }

    private static boolean canBeReverted(@NotNull Transition transition) {
        return transition.getOutputs()
                .stream()
                .allMatch(arc -> arc.getToEndpoint().getMarks() >= arc.weight());
    }
}