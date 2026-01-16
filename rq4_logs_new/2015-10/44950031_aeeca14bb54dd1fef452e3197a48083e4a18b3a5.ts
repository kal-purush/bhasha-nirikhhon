import {
	it,
	describe,
	expect,
	injectAsync,
	beforeEachProviders,
	TestComponentBuilder
} from 'angular2/testing';

import {DraftBoardPage} from './draft_board';
import {DraftBoardComponent} from '../../draft/board/board';
import {Component, provide} from 'angular2/angular2';

@Component({
	selector: 'draft-board',
	template: '<div id="draft-board>This is the draft board</div>'
})
class MockDraftBoardComponent { };


describe('DraftBoardPage', () => {

	beforeEachProviders(() => [DraftBoardPage]);

	it('should display a draft board', injectAsync([TestComponentBuilder], (tcb) => {
		return tcb.overrideDirective(DraftBoardPage, DraftBoardComponent, MockDraftBoardComponent)
			.createAsync(DraftBoardPage).then((fixture) => {
			
			fixture.detectChanges();

			var compiled = fixture.debugElement.nativeElement;
			expect(compiled.querySelector('draft-board')).not.toBeNull();
		});
	}));


});