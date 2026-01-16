/**
 * A simple and flexible inventory system.
 */
export default class Inventory {
	store = {}
	public getTotal(name: string): number {
		return this.store[name];
	}
	/**
	 * Pull items from inventory.
	 */
	public pull(name: string, quantity: number) {
		this.store[name] = this.store[name] - quantity;
	}
	
	/**
	 * Stock item to inventory.
	 */
	public stock(name: string, quantity: number) {
		this.store[name] += quantity;
	}
}