package com.icaynia.noteii;

import android.util.Log;

/**
 * Created by icaynia on 16. 8. 13..
 */
public class memo {
    String note = "-";
    FileManager mFilemnger = new FileManager();

    public void load(String fileName) {
        this.note = mFilemnger.loadFile(fileName).replace("\\n", System.getProperty("line.separator"));

    }

    public String get() {
        return this.note;
    }

    public void set(String _note) {
        this.note = _note;
    }

    public void save(String fileName) {
        mFilemnger.saveFile(fileName, this.note);

        Log.e("Noteii", "File Saved : " + this.note);
    }
}