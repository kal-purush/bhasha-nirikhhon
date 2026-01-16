import * as test from "blue-tape";
import { isDisposable } from "./disposable";

test("isDisposable", async t => {
    t.equal(isDisposable({}), false);
    t.equal(isDisposable({ dispose: null }), false);
    t.equal(isDisposable({ dispose: true }), false);
    t.equal(isDisposable({ dispose: () => null }), true);
});