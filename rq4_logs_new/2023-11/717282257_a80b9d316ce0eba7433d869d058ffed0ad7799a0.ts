import { DocumentData, Firestore, QuerySnapshot, Unsubscribe, collection, deleteDoc, doc, getDoc, getDocs, onSnapshot, query, setDoc, where } from "firebase/firestore";
import { FirestoreApiBase, QueryParam, Storable } from "./FirestoreApiBase";

export class FirestoreApi extends FirestoreApiBase {
    firestore: Firestore;

    constructor(firestore: Firestore, collectionName: string) {
        super(collectionName);
        this.firestore = firestore;
    }

    private getDocRef = (key: string) => {
        const collectionRef = collection(this.firestore, this.collectionName);
        const docRef = doc(collectionRef, key);
        return docRef;
    };

    private getQuery = (path: string, params: QueryParam[]) => {
        const whereClause = params.map(param => where(param.property, param.op, param.value));
        const collectionRef = collection(this.firestore, `${this.collectionName}${path}`);
        return query(collectionRef, ...whereClause);
    };

    protected _set = async (id: string, data: Storable) => {
        const docRef = this.getDocRef(id);
        await setDoc(docRef, data);
    };

    protected _get = async (id: string): Promise<Storable | null> => {
        const docRef = this.getDocRef(id);
        return (await getDoc(docRef)).data() ?? null;
    };

    protected _search = async (path: string, params: QueryParam[]): Promise<QuerySnapshot<DocumentData>> => {
        const query = this.getQuery(path, params);
        return await getDocs(query);
    };

    public delete = async (id: string): Promise<void> => {
        const docRef = this.getDocRef(id);
        await deleteDoc(docRef);
    };

    public searchAndDelete = async (path: string, params: QueryParam[]): Promise<number> => {
        try {
            const snapshot = await this._search(path, params);
            const updates = snapshot.docs.map(doc => deleteDoc(doc.ref));
            await Promise.all(updates);
            return snapshot.docs.length;
        } catch (error) {
            let message = 'an unknown error has occurred';
            if (error instanceof Error) message = error.message;
            console.error(`error deleted documents ${{ path, params }}\nmessage=${message}`);
            return 0;
        }
    };

    public searchAndListen = <T extends Storable>(path: string, params: QueryParam[], callback: (_: T[]) => void, errCallback: (_: unknown) => void, parse: (_: DocumentData) => T): Unsubscribe => {
        try {
            const query = this.getQuery(path, params);
            return onSnapshot(query, docs => {
                try {
                    const retValue: T[] = [];
                    docs.forEach(doc => {
                        const data = parse(doc.data());
                        retValue.push(data);
                    });
                    callback(retValue);
                } catch (error) {
                    errCallback(error);
                }
            });
        } catch (error) {
            let message = 'an unknown error has occurred';
            if (error instanceof Error) message = error.message;
            console.error(`error listening to documents ${{ path, params }}\nmessage=${message}`);
            return () => null;
        }
    };

    public listen = <T extends Storable>(key: string, callback: (_: T) => void, errCallback: (_: unknown) => void, defaultValue: () => T, parse: (_: DocumentData) => T): Unsubscribe => {
        try {
            const docRef = this.getDocRef(key);
            return onSnapshot(docRef, (doc) => {
                try {
                    const data = doc.data();
                    const retValue = (data) ? parse(data) : defaultValue();
                    callback(retValue);
                } catch (err) {
                    errCallback(err);
                }
            });
        } catch (error) {
            let message = 'an unknown error has occurred';
            if (error instanceof Error) message = error.message;
            console.error(`error listening to document ${{ key }}\nmessage=${message}`);
            return () => null;
        }
    };
}