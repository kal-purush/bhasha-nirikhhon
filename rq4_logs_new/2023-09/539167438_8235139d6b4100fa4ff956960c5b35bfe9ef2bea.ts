/**
 * @file This file contains unit tests for code in `../clients.ts`.
 */

import test from "ava";
import * as spec from "../clients";

test("Test ensurePathname with normal and special inputs", (c) => {
  c.plan(3);

  c.deepEqual(spec.ensurePathname("hello"), "/hello");
  c.deepEqual(spec.ensurePathname("/hello"), "/hello");
  c.deepEqual(
    spec.ensurePathname("hello?fake-query#and-fragment"),
    "/hello%3Ffake-query%23and-fragment",
  );
});

test("Test getURLSearchParams with normal and array inputs", (c) => {
  c.plan(4);

  c.deepEqual(spec.getURLSearchParams({}).toString(), "");
  c.deepEqual(
    spec.getURLSearchParams({ hello: "value" }).toString(),
    "hello=value",
  );
  c.deepEqual(
    spec.getURLSearchParams({ hello: ["one", "two"] }).toString(),
    "hello=one&hello=two",
  );
  c.deepEqual(
    spec.getURLSearchParams({ hello: "value", notThis: undefined }).toString(),
    "hello=value",
    "Undefined values should be skipped",
  );
});

test("Test getOutgoingHeaders with numbers", (c) => {
  c.plan(3);

  // Use variable + explicit typing to catch type errors
  let result: Record<string, spec.OutgoingHttpHeader> | undefined;
  result = spec.getOutgoingHeaders(undefined);
  c.deepEqual(result, undefined);
  result = spec.getOutgoingHeaders({ hello: "value", helloNumber: 5 });
  c.deepEqual(result, { hello: "value", helloNumber: 5 });

  result = spec.getOutgoingHeaders({ hello: "value", helloNumbers: [1, 2] });
  c.deepEqual(result, { hello: "value", helloNumbers: ["1", "2"] });
});

test("Test getOutgoingHeaders without numbers", (c) => {
  c.plan(3);

  // Use variable + explicit typing to catch type errors
  let result: Record<string, spec.OutgoingHttpHeaderNoNumber> | undefined;
  result = spec.getOutgoingHeaders(undefined, true);
  c.deepEqual(result, undefined);
  result = spec.getOutgoingHeaders({ hello: "value", helloNumber: 5 }, true);
  c.deepEqual(result, { hello: "value", helloNumber: "5" });

  result = spec.getOutgoingHeaders(
    { hello: "value", helloNumbers: [1, 2] },
    true,
  );
  c.deepEqual(result, { hello: "value", helloNumbers: ["1", "2"] });
});