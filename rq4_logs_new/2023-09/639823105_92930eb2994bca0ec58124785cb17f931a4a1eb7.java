package com.teachsync.utils.enums;

public enum Slot {
    _1(1, "07:00", "08:30"),
    _2(2, "08:45", "10:15"),
    _3(3, "10:30", "12:00"),

    _4(4, "12:30", "14:00"),
    _5(5, "14:15", "15:45"),
    _6(6, "16:00", "17:30"),

    _7(7, "18:00", "19:30"),
    _8(8, "19:45", "21:15");

    private final int slot;
    private final String start;
    private final String end;

    Slot(int slot, String start, String end) {
        this.slot = slot;
        this.start = start;
        this.end = end;
    }

    public int getSlot() {
        return slot;
    }

    public String getStart() {
        return start;
    }

    public String getEnd() {
        return end;
    }
}