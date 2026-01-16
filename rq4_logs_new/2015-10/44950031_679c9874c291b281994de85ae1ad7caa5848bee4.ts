import {Component} from 'angular2/angular2';
import {RouterLink, RouteParams} from 'angular2/router';

import {NFLTeam} from '../../models/nfl_team';
import {NFLTeamService} from '../../../services/nfl_team_service';

@Component({
	selector: 'team-page',
	directives: [RouterLink],
	templateUrl: 'app/components/pages/team/team.html'
})
export class TeamPage {
	team: NFLTeam;

	constructor(private nflTeamService: NFLTeamService, params: RouteParams) { 
		this.team = this.nflTeamService.getTeamByMascot(params.get('mascot'));
	}
}