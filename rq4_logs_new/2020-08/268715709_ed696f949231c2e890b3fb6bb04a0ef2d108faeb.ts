import { devRunner } from "./dev";
import spawn from "cross-spawn";
jest.mock("cross-spawn");
describe("dev script tests", () => {
  test("should call spwan.sync", () => {
    devRunner();
    const sync = jest.spyOn(spawn, "sync");
    expect(sync).toHaveBeenCalledWith(
      "ts-node-dev",
      ["--respawn", "--transpile-only", undefined],
      { stdio: "inherit" },
    );
  });
});