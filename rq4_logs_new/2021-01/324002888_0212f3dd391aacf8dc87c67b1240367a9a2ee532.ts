export interface Student {
    _id: string;
    user: User;
    connected: Teacher[];
    favorites: Teacher[];
}