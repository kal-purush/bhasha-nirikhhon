export type id = number | string;

export interface Data<T> {
    getAll: () => Promise<Array<T>>;
    get: (id: id) => Promise<T>;
    post: (data: Partial<T>) => Promise<T>;
    patch: (id: id, data: Partial<T>) => Promise<T>;
    delete: (id: id) => Promise<{ id: id }>;
}

export interface BasicData<T> {
    get: (id: id) => Promise<T>;
    post: (data: Partial<T>) => Promise<T>;
    findOne: (data: any) => Promise<T>;
}

export interface ExtraData<T> {
    getAll: () => Promise<Array<T>>;
    patch: (id: id, data: Partial<T>) => Promise<T>;
    delete: (id: id) => Promise<void>;
}