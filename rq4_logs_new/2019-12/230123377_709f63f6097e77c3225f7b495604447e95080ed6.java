package ru.nuclearreactor.forceofworlds.models;

import java.util.ArrayList;
import java.util.List;

import ru.nuclearreactor.forceofworlds.entities.*;

public class CurrentPage {
    private static Page currentPage;
    private static List<subscribe<Page>> subs = new ArrayList<>();

    public static void sub(subscribe sub) {
        subs.add(sub);
        sub.action(currentPage);
    }

    public static void set(Page newPage) {
        currentPage = newPage;
        onUpdate();
    }

    private static void onUpdate() {
        for (subscribe<Page> sub: subs) {
            sub.action(currentPage);
        }
    }
}