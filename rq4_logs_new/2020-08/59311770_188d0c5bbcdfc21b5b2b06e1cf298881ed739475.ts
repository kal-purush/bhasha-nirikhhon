import { expect } from 'chai';
import { startTestServer } from './server/test-server';
import { PageObject } from './pages/page-object';
import { TeamCreatePage } from './pages/team-create-page';
// import { TeamRosterPage } from './pages/team-roster-page';

describe('functional tests', function () {
  let server: any;
  let pageObject: PageObject;

  before(async function () {
    server = await startTestServer();
  });

  after((done) => server.close(done));

  afterEach(async () => {
    await pageObject?.close();
  });

  it('creates new team', async function () {
    const addTeamPage = pageObject = new TeamCreatePage();
    await addTeamPage.init();
    await addTeamPage.open();

    await addTeamPage.fillTeamDetails('A functional team');
    await addTeamPage.saveNewTeam();

    // Verifies that the new team is created and set as the current team.
    const currentTeam = await addTeamPage.getCurrentTeam();
    expect(currentTeam?.name).to.equal('A functional team', 'Newly-created team name');
  });
});