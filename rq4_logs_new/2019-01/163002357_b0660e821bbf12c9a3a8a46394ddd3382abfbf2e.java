package org.altervista.girildo.table;

import org.altervista.girildo.VoteCategory;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.HashMap;

class TableRow implements Comparable<TableRow> {
    private HashMap<VoteCategory, Integer> points;

    TableRow(Collection<VoteCategory> categories) {
        this.points = new HashMap<>();
        this.points.put(VoteCategory.SPECIAL_TOTAL_CATEGORY, 0);

        for (VoteCategory category : categories)
            this.points.put(category, 0);
    }

    void addPoints(VoteCategory category, int points) throws NullPointerException{
        if(!this.points.containsKey(category))
            throw new NullPointerException(String.format("This category (%s) does not exist!", category.toString()));
        this.points.merge(category, points, Integer::sum);
        if (!category.equals(VoteCategory.SPECIAL_TOTAL_CATEGORY))
            this.points.merge(VoteCategory.SPECIAL_TOTAL_CATEGORY, points, Integer::sum);
    }

    @Override
    public int compareTo(@NotNull TableRow o) {
        return this.points.get(VoteCategory.SPECIAL_TOTAL_CATEGORY)
                .compareTo(o.points.get(VoteCategory.SPECIAL_TOTAL_CATEGORY));
    }
}