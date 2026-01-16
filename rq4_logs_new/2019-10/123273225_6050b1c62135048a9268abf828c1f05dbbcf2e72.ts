/* tslint:disable:ordered-imports no-implicit-dependencies */
import test, { ExecutionContext, Implementation } from "ava";
import { parseAttribute } from "../src/parser";
// @ts-ignore
import { omitBy , isUndefined} from "lodash";
// @ts-ignore
// tslint:disable-next-line:no-var-requires
const fixtures = require("./fixtures/attributes2schema.json");

fixtures.forEach((fixture: any) => {
  test(fixture.title, (t: ExecutionContext) => {
    // as the parser can return undefined for various properties we ignore these to make defining the tests easier
    t.deepEqual(omitBy(parseAttribute(fixture.input), isUndefined), fixture.output);
  });
});