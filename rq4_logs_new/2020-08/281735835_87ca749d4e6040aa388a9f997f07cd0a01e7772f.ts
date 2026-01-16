import {getNewUrls} from './getNewUrls';

const assertUrls = (actual: URL[], expected: URL[]) => {
  expect(actual).toHaveLength(expected.length);
  expected.forEach((e) => {
    expect(actual).toContainEqual<URL>(e);
  });
};

describe('getNewUrls', () =>{
  test('Url Already Exists - Returns Nothing', () =>{
    const existingUrls = new Map<string, URL[]>();
    existingUrls.set('bleacherreport.com', [new URL('https://bleacherreport.com/dallas-cowboys'), new URL('https://bleacherreport.com/nfl')]);

    const newUrls = [new URL('https://bleacherreport.com/dallas-cowboys')];

    const expected : URL[] = [];

    const actual = getNewUrls(existingUrls, newUrls);

    assertUrls(actual, expected);
  });

  test('New Host - Returns New URL', () =>{
    const existingUrls = new Map<string, URL[]>();
    existingUrls.set('bleacherreport.com', [new URL('https://bleacherreport.com/dallas-cowboys'), new URL('https://bleacherreport.com/nfl')]);

    const newUrls = [new URL('https://us.cnn.com/')];

    const expected : URL[] = [new URL('https://us.cnn.com/')];

    const actual = getNewUrls(existingUrls, newUrls);

    assertUrls(actual, expected);
  });

  test('Same Host, New URL - Returns New URL', () =>{
    const existingUrls = new Map<string, URL[]>();
    existingUrls.set('bleacherreport.com', [new URL('https://bleacherreport.com/dallas-cowboys'), new URL('https://bleacherreport.com/nfl')]);

    const newUrls = [new URL('https://bleacherreport.com/rugby')];

    const expected : URL[] = [new URL('https://bleacherreport.com/rugby')];

    const actual = getNewUrls(existingUrls, newUrls);

    assertUrls(actual, expected);
  });
});