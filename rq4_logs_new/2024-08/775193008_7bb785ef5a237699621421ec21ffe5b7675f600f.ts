import { Note } from "./Note"

export interface MusicSheet {
    musicName: string,
    globalNoteSpeed: number,
    // notes: NoteData[]
}

export interface NoteData {
    tonality: Note,
    /**
     * When this note should spawn, relative to the beggining of the music, in ms
     */
    spawnsAt?: number,
    /**
     * When this note should reach the end of the sheet, relative to the beggining of the music, in ms
     */
    endsAt?: number,
    /**
     * individual speed modifier for this note
     */
    individualSpeed?: number
}