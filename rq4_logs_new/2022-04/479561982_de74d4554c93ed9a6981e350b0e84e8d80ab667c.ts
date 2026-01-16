import { Injectable } from '@angular/core';
import { ActivatedRoute, Params, Router } from '@angular/router';
import { Observable } from 'rxjs';
import { Mode } from '../interfaces';

@Injectable({
	providedIn: 'root',
})
export class RouterService {
	constructor(private readonly _router: Router, private readonly _route: ActivatedRoute) {}

	setParams(mode: Mode) {
		this._router.navigate([], {
			queryParams: {
				mode,
			},
		});
	}

	get navigate$(): Observable<Params> {
		return this._route.queryParams;
	}
}