export default class QuantityMap<T> {
    private map: Map<T, number> = new Map<T, number>([]);

    constructor(){}

    add(key: T, quantity: number) {
        const oldQuantity = this.get(key);
        const newQuantity = oldQuantity + quantity;
        if (newQuantity < 0) {
            throw 'Tried to set a quantity map to a negative value';
        }
        this.map.set(key, newQuantity);
    }

    remove(key: T, quantity: number) {
        this.add(key, quantity * -1);
    }

    get(key: T): number {
        return this.map.get(key) || 0;
    }

    // returns all keys with nonzero quantities
    getKeys() {
        return Array.from(this.map.keys()).filter(key => (this.map.get(key)));
    }
}