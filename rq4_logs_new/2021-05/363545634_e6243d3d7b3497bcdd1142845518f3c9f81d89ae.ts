import nock from "nock";
import {Awair, MOCK_USER} from "./source";

test("Create an instance", async () => {
    const awair = new Awair();
    expect(awair instanceof Awair).toEqual(true);
});

test("Get user", async () => {
    const scope = nock(/.*/).get("/v1/users/self").reply(200, MOCK_USER);

    const awair = new Awair();
    const user = await awair.getUser();

    expect(scope.isDone()).toEqual(true);
    expect(user.id).toEqual(MOCK_USER.id);
});

test("Retries getting a user", async () => {
    const scope = nock(/.*/)
        .get("/v1/users/self")
        .reply(500, "server error")
        .get("/v1/users/self")
        .reply(200, MOCK_USER);

    const awair = new Awair();
    const user = await awair.getUser();

    expect(scope.isDone()).toEqual(true);
    expect(user.id).toEqual(MOCK_USER.id);
});