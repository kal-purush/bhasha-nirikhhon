import { Firestore } from "firebase/firestore";
import { FirestoreApi } from ".";

export type User = {
    invitations: string[];
    joined: string[];
};

export class UsersApi {
    readonly api: FirestoreApi;

    constructor(db: Firestore) {
        this.api = new FirestoreApi(db, 'users');
    }

    generateNewUser = async (id: string, email: string | null): Promise<string | null> => this.api.set(id, { invitations: [], joined: [], email });

    deleteUser = async (id: string): Promise<void> => this.api.delete(id);
};