package com.opentable.components.filterorder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

class CycleFinder<T> {
    private final Set<T> seen = new HashSet<>();
    private final Map<T, Collection<T>> edges;

    CycleFinder(final Map<T, Collection<T>> edges) {
        this.edges = edges;
    }

    Optional<List<T>> run() {
        for (final T node : edges.keySet()) {
            final Optional<List<T>> cycle = dfs(node);
            if (cycle.isPresent()) {
                return cycle;
            }
        }
        return Optional.empty();
    }

    private Optional<List<T>> dfs(final T start) {
        seen.clear();
        final List<T> cycle = findCycle(start);
        if (cycle.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(cycle);
    }

    private List<T> findCycle(final T node) {
        boolean newlySeen = seen.add(node);
        if (!newlySeen) {
            // Cycle!
            final List<T> cycle = new ArrayList<>();
            cycle.add(node);
            return cycle;
        }
        // We need to go deeper.
        for (final T next : edges.getOrDefault(node, Collections.emptySet())) {
            final List<T> cycle = findCycle(next);
            if (!cycle.isEmpty()) {
                cycle.add(node);
                return cycle;
            }
            // If no cycle found in the just-finished recursion, remove from seen
            seen.remove(next);
        }
        // No cycle found at this point.
        return Collections.emptyList();
    }
}