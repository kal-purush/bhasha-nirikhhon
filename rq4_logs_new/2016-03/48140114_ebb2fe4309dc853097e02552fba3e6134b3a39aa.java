package by.bsuir.myfullviolin.model.note;

/**
 * Enum for menu in spinner in notes activity.
 */
public enum NoteMenu {
    PLAYNOTES("PlayNotes"),
    VIEWNOTES("ViewNotes"),
    SETRINGTONE("SetRingtone"),
    SETNOTIFICATION("SetNotification");

    private String name;
    NoteMenu(String name){this.name=name;}

    @Override
    public String toString() {
        return name;
    }

    public String getNoteMenuName() {
        return name;
    }
}