export declare class KeyValuePair<T, U> {
    key: T;
    value: U;
    constructor(key: T, value: U);
}
export declare class PriorityQueue<T> {
    private nodes;
    enqueue(priority: number, value: T): void;
    dequeue(): T;
    sort(): void;
    isEmpty(): boolean;
}
export declare class Vector2 {
    x: number;
    y: number;
    constructor(x: number, y: number);
    distance(v: Vector2): number;
    angle(v: Vector2): number;
    toString(): string;
}
export declare class Vertex {
    identifier: string;
    edges: {
        [vertex: string]: KeyValuePair<Vertex, number>;
    };
    private _position;
    position: Vector2;
    constructor(identifier: string, position: Vector2);
    addNeighbor(neighbor: Vertex, distance: number): void;
    equals(other: Vertex): boolean;
}