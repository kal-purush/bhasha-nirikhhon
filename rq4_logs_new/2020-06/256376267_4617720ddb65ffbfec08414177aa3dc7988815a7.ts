/* global Deno */
import {
  RouterContext,
  Context,
  Application,
  RouteParams,
  Router,
} from "https://deno.land/x/oak/mod.ts";
import { assertEquals } from "https://deno.land/std/testing/asserts.ts";
import {
  spy,
  resolves,
  Spy,
} from "https://raw.githubusercontent.com/udibo/mock/master/mod.ts";

import { UserHandler } from "./userHandler.ts";
import { IUserRepository } from "./userRepository.ts";

function createMockApp<
  S extends Record<string | number | symbol, any> = Record<string, any>,
>(
  state = {} as S,
): Application<S> {
  const app = {
    state,
    use() {
      return app;
    },
  };
  return app as any;
}

function createMockContext<
  S extends Record<string | number | symbol, any> = Record<string, any>,
>(
  app: Application<S>,
  body: Record<string, any> = {},
) {
  const headers = new Headers();
  return ({
    app,
    request: {
      headers: new Headers(),
      method: "POST",
      body: () => Promise.resolve({ type: "json", value: body }),
      url: new URL("https://localhost/"),
    },
    response: {
      status: undefined,
      body: undefined,
      redirect(url: string | URL) {
        headers.set("Location", encodeURI(String(url)));
      },
      headers,
    },
    state: app.state,
  } as unknown) as Context<S>;
}

const asRouterContext = <P extends RouteParams, S>(
  c: Context<S>,
  params: P,
) => {
  const ctx = c as RouterContext<P, S>;
  ctx.captures = [];
  ctx.router = new Router();
  ctx.params = params;
  return ctx;
};

const mockRepo: IUserRepository = {
  getUser: spy() as Spy<any>,
  getUsers: spy(),
  createUser: spy(),
};

Deno.test({
  name: "userHandler getUsers",
  async fn() {
    const handler = new UserHandler(mockRepo);
    mockRepo.getUsers = spy(resolves(["test"]));
    const ctx = createMockContext(createMockApp());
    await handler.getUsers(ctx);
    assertEquals(ctx.response.body, ["test"]);
    assertEquals((mockRepo.getUsers as Spy<any>).calls.length, 1);
  },
  // sanitizeOps: false,
});

Deno.test("userHandler getUser one", async () => {
  const handler = new UserHandler(mockRepo);
  mockRepo.getUser = spy(resolves("one user"));
  const ctx = asRouterContext(
    createMockContext(createMockApp()),
    { id: "1" } as Record<string | number, string | undefined>,
  );
  await handler.getUser(ctx);
  assertEquals(ctx.response.body, "one user");
  assertEquals((mockRepo.getUser as Spy<any>).calls.length, 1);
  assertEquals((mockRepo.getUser as Spy<any>).calls[0].args, [1]);
});

Deno.test("userHandler createUser success", async () => {
  const handler = new UserHandler(mockRepo);
  mockRepo.getUser = spy(resolves("one user"));
  mockRepo.createUser = spy(resolves(2));
  const ctx = createMockContext(createMockApp(), { name: "abc" });
  await handler.createUser(ctx);
  assertEquals(ctx.response.body, "one user");
  assertEquals((mockRepo.createUser as Spy<any>).calls[0].args, ["abc"]);
  assertEquals((mockRepo.getUser as Spy<any>).calls.length, 1);
  assertEquals((mockRepo.getUser as Spy<any>).calls[0].args, [2]);
});