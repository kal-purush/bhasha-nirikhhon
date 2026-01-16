package planmysem.data.semester;

import java.time.DayOfWeek;
import java.util.List;

import planmysem.data.slot.Slot;

/**
 * Represents a Day in the planner.
 * Guarantees: details are present and not null, field values are validated.
 */
public class Day {
    private String name;
    private DayOfWeek dayOfWeek;
    private List<Slot> slots;

    /**
     * Assumption: Every field must be present and not null.
     */
    public Day(String name, DayOfWeek dayOfWeek) {
        this.name = name;
        this.dayOfWeek = dayOfWeek;
    }

    public String getName() {
        return name;
    }

    public DayOfWeek getDayOfWeek() {
        return dayOfWeek;
    }

    public List<Slot> getSlots() {
        return slots;
    }

}