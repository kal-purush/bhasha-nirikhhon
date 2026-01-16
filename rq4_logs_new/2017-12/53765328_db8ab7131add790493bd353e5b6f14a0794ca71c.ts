import { TestBed, inject } from '@angular/core/testing';

import { MainPageComponent } from './main-page.component';

describe('a main-page component', () => {
	let component: MainPageComponent;

	// register all needed dependencies
	beforeEach(() => {
		TestBed.configureTestingModule({
			providers: [
				MainPageComponent
			]
		});
	});

	// instantiation through framework injection
	beforeEach(inject([MainPageComponent], (MainPageComponent) => {
		component = MainPageComponent;
	}));

	it('should have an instance', () => {
		expect(component).toBeDefined();
	});
});