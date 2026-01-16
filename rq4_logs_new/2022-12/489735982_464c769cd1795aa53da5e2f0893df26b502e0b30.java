package ua.masaltsev.webnotes.entity;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class NoteTest {

    public static final int ID = 3;
    public static final String TITLE = "Shopping";
    public static final String CONTENTS = "Pants";
    private Note note;

    @BeforeEach
    void setUp() {
        note = new Note();
        note.setId(ID);
        note.setTitle(TITLE);
        note.setContents(CONTENTS);
    }

    @Test
    void getId() {
        assertEquals(ID, note.getId());
    }

    @Test
    void getTitle() {
        assertEquals(TITLE, note.getTitle());
    }

    @Test
    void getContents() {
        assertEquals(CONTENTS, note.getContents());
    }

    @Test
    void setId() {
    }

    @Test
    void setTitle() {
    }

    @Test
    void setContents() {
    }
}