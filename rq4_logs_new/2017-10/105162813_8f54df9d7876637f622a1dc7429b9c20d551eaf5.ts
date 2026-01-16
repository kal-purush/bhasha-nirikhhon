import * as $ from 'jquery';
import NoteList from './NoteList';
import NoteBodyInput from './NoteBodyInput';
import NoteTitleInput from './NoteTitleInput';
import StatusBar from './StatusBar';
import NoteRepository from './NoteRepository';

export default class App {

  repository: NoteRepository;

  noteList: NoteList;
  titleInput: NoteTitleInput;
  bodyInput: NoteBodyInput;
  statusBar: StatusBar;
  deleteButton: JQuery;

  private openedNoteId: string|null = null;
  private originalNote: {title: string, body: string} = {title: '', body: ''};

  constructor() {
    this.repository = new NoteRepository();

    this.noteList = new NoteList();
    this.titleInput = new NoteTitleInput();
    this.bodyInput = new NoteBodyInput();
    this.statusBar = new StatusBar();
    this.deleteButton = $('#note-delete-button');
  }

  public init() {
    this.registerKeyEventHandler();
    this.changeOpenedNote(null);
    this.titleInput.focus();
    this.noteList.refresh();
    this.setupNoteDeleteButton();
  }

  private registerKeyEventHandler(): void {
    document.addEventListener('keydown', (e: KeyboardEvent) => {
      // console.log(e);
      if (e.ctrlKey && (e.key === 's' || e.key === 'S')) {
        return this.saveNote().then(() => {
          return this.noteList.refresh();
        });
      } else if (e.ctrlKey && (e.key === 'n' || e.key === 'N')) {
        return this.saveNote().then(() => {
          this.changeOpenedNote(null);
          this.titleInput.focus();
          return this.noteList.refresh();
        });
      }
      return;
    });
  }

  public saveNote(): Promise<any> {
    const title = this.titleInput.getValue();
    const body = this.bodyInput.getValue();
    if (!this.isNoteModified()) {
      // do nothing
      return Promise.resolve();
    }
    if (this.openedNoteId === null) {
      return this.repository.add(title, body).then(id => {
        return this.repository.get(id);
      }).then(note => {
        this.changeOpenedNote(note);
        return;
      });
    }
    return this.repository.update(this.openedNoteId, title, body).then(() => {
      this.updateOriginalNote();
    });
  }

  // open a new note when note parameter is null
  public changeOpenedNote(note: any): void {
    if (note !== null) {
      this.openedNoteId = note._id;
      this.titleInput.setValue(note.title);
      this.bodyInput.setValue(note.body);
      this.updateOriginalNote();
      this.deleteButton.show();
      this.statusBar.update(`id: ${note._id}`);
    } else {
      this.openedNoteId = null;
      this.titleInput.setValue('');
      this.bodyInput.setValue('');
      this.updateOriginalNote();
      this.deleteButton.hide();
      this.statusBar.update('New Note');
    }
  }

  public updateOriginalNote(): void {
    this.originalNote = {
      title: this.titleInput.getValue(),
      body: this.bodyInput.getValue(),
    };
  }

  public getOpenedNoteId(): string|null {
    return this.openedNoteId;
  }

  public isNoteModified(): boolean {
    return this.originalNote.title !== this.titleInput.getValue() || this.originalNote.body !== this.bodyInput.getValue();
  }

  private setupNoteDeleteButton(): void {
    this.deleteButton.on('click', () => {
      if (window.confirm('Delete this note?')) {
        if (this.openedNoteId === null) {
          throw new Error();
        }
        return this.repository.remove(this.openedNoteId).then(() => {
          this.changeOpenedNote(null);
          this.titleInput.focus();
          return this.noteList.refresh();
        });
      }
      return;
    });
  }
}