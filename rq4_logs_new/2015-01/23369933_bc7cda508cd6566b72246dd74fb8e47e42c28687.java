package chapter6.exercise01;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class LongestString {
    private AtomicReference<String> longestString;

    public LongestString(String initialString) {
        Objects.requireNonNull(initialString, "string must not be null");
        this.longestString = new AtomicReference<>(initialString);
    }

    public void update(String newString) {
        longestString.updateAndGet(oldString -> {
           return newString.length() >= oldString.length() ? newString : oldString;
        });
    }

    public String get() {
        return longestString.get();
    }
}