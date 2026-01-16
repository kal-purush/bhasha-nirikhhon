class DatabaseModel {
    id: number = 0;
    created_at: Date = new Date();
    updated_at: Date = new Date();
}

export class Entry extends DatabaseModel {
    content: string = "";
    title: string = "";
}