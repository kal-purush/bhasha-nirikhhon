package org.openhds.mobile.task;

import android.content.Context;

public class TaskStatus {

    private int state;
    private Integer progress;

    public TaskStatus(int state) {
        this(state, null);
    }

    public TaskStatus(int state, Integer progress) {
        this.state = state;
        this.progress = progress;
    }

    public String format(Context ctx) {
        CharSequence stateText = ctx.getText(state);
        return progress == null ? stateText.toString() : String.format("%s %3d%%", stateText, progress);
    }

}