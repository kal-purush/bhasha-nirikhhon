import {provide} from 'angular2/angular2';

import {Pick} from './pick';
import {FantasyTeam} from '../fantasy_team/fantasy_team';
import {FantasyTeamService} from '../../services/fantasy_team_service';

class MockFantasyTeamService extends FantasyTeamService {
	size(): number {
		return 8;
	}
}

describe("Pick", () => {

	describe("isPicked()", () => {

		it('is false without a team', () => {
			var pick: Pick = new Pick(1, null);
			expect(pick.isPicked()).toBe(false);
		});

		it('is true with a team', () => {
			var pick: Pick = new Pick(1, null);
			pick.team = new FantasyTeam();
			expect(pick.isPicked()).toBe(true);
		});

	});

	describe("round", () => {

		it('is 1 for the 1st pick in an 8 man league', () => {
			var pick = new Pick(1, new MockFantasyTeamService());
			expect(pick.round).toBe(1);
		});

		it('is 1 for the 8th pick in an 8 man league', () => {
			var pick = new Pick(8, new MockFantasyTeamService());
			expect(pick.round).toBe(1);
		});

		it('is 2 for the 9th pick in an 8 man league', () => {
			var pick = new Pick(9, new MockFantasyTeamService());
			expect(pick.round).toBe(2);
		});

		it('is 16 for the last pick in an 8 man league', () => {
			var pick = new Pick(16*8, new MockFantasyTeamService());
			expect(pick.round).toBe(16);
		});

	});

	describe("slot", () => {

		it('is 1 for the 1st pick in an 8 man league', () => {
			var pick = new Pick(1, new MockFantasyTeamService());
			expect(pick.slot).toBe(1);
		});

		it('is 8 for the 8th pick in an 8 man league', () => {
			var pick = new Pick(8, new MockFantasyTeamService());
			expect(pick.slot).toBe(8);
		});

		it('is 1 for the 9th pick in an 8 man league', () => {
			var pick = new Pick(9, new MockFantasyTeamService());
			expect(pick.slot).toBe(1);
		});

		it('is 8 for the last pick in an 8 man league', () => {
			var pick = new Pick(16 * 8, new MockFantasyTeamService());
			expect(pick.slot).toBe(8);
		});

	});

	describe("spot", () => {

		it('is "1.1" for the 1st pick in an 8 man league', () => {
			var pick = new Pick(1, new MockFantasyTeamService());
			expect(pick.spot).toBe("1.1");
		});

		it('is "3.6" for the 22nd pick in an 8 man league', () => {
			var pick = new Pick(22, new MockFantasyTeamService());
			expect(pick.spot).toBe("3.6");
		});

	})

});