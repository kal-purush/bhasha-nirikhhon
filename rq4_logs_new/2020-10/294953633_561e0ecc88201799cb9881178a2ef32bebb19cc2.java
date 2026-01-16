package jimmy.mcgymmy.model;

import java.util.EmptyStackException;
import java.util.Stack;
import java.util.function.Predicate;

import javafx.util.Pair;
import jimmy.mcgymmy.model.food.Food;

class History {
    private final Stack<Pair<McGymmy, Predicate<Food>>> stack;

    History() {
        stack = new Stack<>();
    }

    private boolean checkIfSameWithPreviousState(McGymmy otherMcGymmy, Predicate<Food> otherPredicate) {
        if (stack.empty()) {
            return false;
        }

        McGymmy mcGymmy = stack.peek().getKey();
        Predicate<Food> predicate = stack.peek().getValue();

        boolean isSameMcGymmy = otherMcGymmy.equals(mcGymmy);
        boolean isSamePredicate = otherPredicate.equals(predicate);
        return isSameMcGymmy && isSamePredicate;
    }

    void save(ModelManager modelManager) {
        McGymmy mcGymmy = new McGymmy(modelManager.getMcGymmy());
        Predicate<Food> predicate = modelManager.getFilterPredicate();
        boolean isSameWithPreviousState = checkIfSameWithPreviousState(mcGymmy, predicate);
        if (!isSameWithPreviousState) {
            stack.push(new Pair<>(mcGymmy, predicate));
        }
    }

    boolean empty() {
        return stack.empty();
    }

    Pair<McGymmy, Predicate<Food>> pop() throws EmptyStackException {
        assert !stack.empty() : "History is empty";
        return stack.pop();
    }
}
