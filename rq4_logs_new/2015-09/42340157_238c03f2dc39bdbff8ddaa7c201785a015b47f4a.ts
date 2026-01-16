import NeDBDataStore from 'nedb';

export default class Data {
	
	store: NeDBDataStore;
	
	constructor(file: string) {
		this.store = new NeDBDataStore({
			filename: file
		});
	}
	
	connect(done: (error: Error) => void) {
		this.store.loadDatabase(done);
	}
}