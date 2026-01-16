// import faker from "faker";
import { graphqlTestCall, debugResponse } from "../utils/graphqlTestCall";
import { dbUp } from "../utils/testDbOps";
import {
  createTestTeam,
  createAdminUser,
  createTestUser,
  createTestOLPermission
} from "../utils/createTestEntities";

const QUERY = `
query summaryCountAllUsers {
  summaryCountAllUsers
}
`;

beforeEach(async () => {
  await dbUp();
});

describe("Summary Count orgs users", () => {
  test("Happy Path", async () => {
    const adminUser = await createAdminUser();
    const user2 = await createTestUser();
    const user3 = await createTestUser();
    const user4 = await createTestUser();
    const user5 = await createTestUser();
    const user6 = await createTestUser();
    const team = await createTestTeam();
    const team2 = await createTestTeam();

    await createTestOLPermission(adminUser.id, team.id, "ADMIN");
    await createTestOLPermission(user2.id, team.id, "MEMBER");
    await createTestOLPermission(user3.id, team.id, "MEMBER");
    await createTestOLPermission(user4.id, team.id, "MEMBER");
    await createTestOLPermission(user5.id, team2.id, "MEMBER");
    await createTestOLPermission(user6.id, team2.id, "APPLICANT"); // holdout

    const response = await graphqlTestCall(
      QUERY,
      { teamId: team.id },
      { user: { id: adminUser.id } }
    );
    debugResponse(response);
    expect(response.data.summaryCountAllUsers).toEqual(5);
  });

  // Must be team admin or app admin
  test("PermCheck no team admin", async () => {
    const adminUser = await createAdminUser();
    const user2 = await createTestUser();
    const user3 = await createTestUser();
    const user4 = await createTestUser();
    const user5 = await createTestUser();
    const team = await createTestTeam();
    const team2 = await createTestTeam();

    await createTestOLPermission(adminUser.id, team.id, "ADMIN");
    await createTestOLPermission(user2.id, team.id, "MEMBER");
    await createTestOLPermission(user3.id, team.id, "MEMBER");
    await createTestOLPermission(user4.id, team.id, "MEMBER");
    await createTestOLPermission(user5.id, team2.id, "MEMBER"); // holdout

    const response = await graphqlTestCall(
      QUERY,
      { teamId: team.id },
      { user: { id: user5.id } }
    );
    debugResponse(response);
    expect(response.data).toBeNull();
    expect(response.errors.length).toEqual(1);
    expect(response.errors[0].message).toEqual("Not Authorized!");
  });
});