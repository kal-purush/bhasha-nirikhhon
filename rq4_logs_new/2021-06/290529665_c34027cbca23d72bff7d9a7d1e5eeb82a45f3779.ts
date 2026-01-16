type StorableMock = Record<string, unknown>;

export default class MockStore {
    private readonly _dataInst: StorableMock;

    constructor(initState: StorableMock) {
        this._dataInst = initState;
    }

    public getData<T = StorableMock>(key: string) {
        const data = this._dataInst[key] as T;
        console.log(data);
        return Promise.resolve(data);
    }

    public setData(key: string, value: StorableMock) {
        this._dataInst[key] = value;
        console.log({ key, value });
        return Promise.resolve();
    }

    public logDb() {
        console.log(JSON.stringify(this._dataInst));
    }
}