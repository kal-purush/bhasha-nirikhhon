import {provide} from 'angular2/angular2';
import {
	it,
	describe,
	expect,
	inject,
	injectAsync,
	beforeEachProviders
} from 'angular2/testing';

import {PicksService} from './picks_service';
import {FantasyTeamService} from './fantasy_team_service';
import {FantasyTeam} from '../models/fantasy_team/fantasy_team';


class MockFantasyTeamService extends FantasyTeamService {
	get teams() {
		var teams: Array<FantasyTeam> = [
			{ owner: 'Team 1' },
			{ owner: 'Team 2' },
		];
		return Promise.resolve(teams);
	}
}

describe('PicksService', () => {

	beforeEachProviders(() => [
		provide(FantasyTeamService, { useClass: MockFantasyTeamService }),
		PicksService,
	]);

	describe('construction', () => {

		it('creates picks from teams', injectAsync([PicksService], (service) => {
			return new Promise((resolve, reject) => {
				setTimeout(() => {
					resolve(expect(service.picks[0].team.owner).toEqual('Team 1'));
				}, 100);
			});
		}), 1000);

		it('divies up the picks', injectAsync([PicksService], (service) => {
			return new Promise((resolve, reject) => {
				setTimeout(() => {
					resolve(expect(service.picks[1].team.owner).toEqual('Team 2'));
				}, 100);
			});
		}), 1000);

		it('snakes alternate rounds', injectAsync([PicksService], (service) => {
			return new Promise((resolve, reject) => {
				setTimeout(() => {
					resolve(expect(service.picks[3].team.owner).toEqual('Team 1'));
				}, 100);
			});
		}), 1000);

		it('creates 16 rounds', injectAsync([PicksService], (service) => {
			return new Promise((resolve, reject) => {
				setTimeout(() => {
					resolve(expect(service.picks.length).toEqual(32));
				}, 100);
			});
		}), 1000);

		it('sets the pick number', injectAsync([PicksService], (service) => {
			return new Promise((resolve, reject) => {
				setTimeout(() => {
					resolve(expect(service.picks[0].number).toEqual(1));
				}, 100);
			});
		}), 1000);



	})

	describe('currentPickNumber', () => {

		it('defaults to 1', inject([PicksService], (service) => {
			expect(service.currentPickNumber).toEqual(1);
		}));

	});

});