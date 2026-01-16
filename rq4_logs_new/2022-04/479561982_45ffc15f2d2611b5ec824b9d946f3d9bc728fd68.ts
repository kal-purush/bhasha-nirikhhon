import { Injectable } from '@angular/core';
import { City, StateTable } from '../interfaces';
import { BehaviorSubject, Observable } from 'rxjs';

@Injectable({
	providedIn: 'root',
})
export class StateTableService {
	private readonly initState: StateTable = {
		header: [],
		cities: [],
	};
	private readonly _stateTable = new BehaviorSubject<StateTable>(this.initState);

	setState(header: string[], city: City) {
		const state: StateTable = this._stateTable.getValue();
		const newState: StateTable = { ...state, header, cities: [...state.cities, city] };
		this._stateTable.next(newState);
	}

	get state$(): Observable<StateTable> {
		return this._stateTable.asObservable();
	}

	clearState() {
		this._stateTable.next(this.initState);
	}
}