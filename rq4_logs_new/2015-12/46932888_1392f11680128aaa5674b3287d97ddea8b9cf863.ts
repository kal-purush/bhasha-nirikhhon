declare class Greeter {
    greeting: string;
    constructor(message: string);
    greet(): string;
}
declare function randomString(length?: number): string;
interface INote {
    id: string;
    title?: string;
    content?: string;
    posX?: number;
    posY?: number;
}
interface IDictionary<TValue> {
    [id: string]: TValue;
}
interface IBoard {
    name?: string;
    color?: string;
    notes?: IDictionary<INote>;
}
interface INoteVMStyle {
    top: string;
    left: string;
    display: string;
}
declare class NoteVM {
    id: string;
    title: KnockoutObservable<string>;
    content: KnockoutObservable<string>;
    posX: KnockoutObservable<number>;
    posY: KnockoutObservable<number>;
    style: KnockoutComputed<INoteVMStyle>;
    update(plain: INote): void;
    toPlain(): INote;
}
declare function AddTruthyValue(destination: any, key: string, value: any): void;
declare function AddNumberValue(destination: any, key: string, value: number): void;
declare class BoardVM {
    name: KnockoutObservable<string>;
    color: KnockoutObservable<string>;
    notes: KnockoutObservableArray<NoteVM>;
    private notesById;
    newNote: () => NoteVM;
    createNote(id?: string): NoteVM;
    deleteNote(id: string): void;
    update(plain: IBoard): void;
    toPlain(): IBoard;
}
declare var shadowServer: {};
declare var board: BoardVM;
declare var shadowClient: IBoard;
declare var io: any;
declare var socket: any;
declare var interval: number;