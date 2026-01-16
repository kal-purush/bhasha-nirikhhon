import {Component, CORE_DIRECTIVES} from 'angular2/angular2';

import {Pick} from '../../models/pick';
import {PickComponent} from '../pick/pick';
import {PicksService} from '../../services/picks_service'
import {FantasyTeamService} from '../../services/fantasy_team_service'

@Component({
	selector: 'draft-list',
	directives: [CORE_DIRECTIVES, PickComponent],
	templateUrl: 'app/components/draft_list/draft_list.html'
})
export class DraftListComponent {
	picks: Array<Pick>;
	
	constructor(public picksService : PicksService) { 
		this.picks = this.picksService.picks;
	}

}