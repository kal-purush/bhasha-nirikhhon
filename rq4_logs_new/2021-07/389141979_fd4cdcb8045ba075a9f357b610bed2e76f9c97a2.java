package com.example.notehw.core.datasources;

import com.example.notehw.core.entities.Note;

public interface NotesSource {
    Note getNote(int position);

    int getSize();
}