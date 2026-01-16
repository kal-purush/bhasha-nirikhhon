import {
  TestFramework,
  Test,
  testFramework,
  removeWhiteSpace
} from '../../testHelpers';
import LogIn from '../../../../src/Home/LogIn';
let expectedValue;
let actualValue;

testFramework.runTests(['Home', 'LogIn','render'], [
  () => {
    const logIn = new LogIn();
    logIn.googleUrl = 'foo';
    actualValue = removeWhiteSpace(logIn.render());
    expectedValue = "<a href=foo>Sign up with Google</a>";

    testFramework.it(
      'returns a link with the Google link',
      new Test(actualValue).equals(expectedValue)
    );
  }
]);