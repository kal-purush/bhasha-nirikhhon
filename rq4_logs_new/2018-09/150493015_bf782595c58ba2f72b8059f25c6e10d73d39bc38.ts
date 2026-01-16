import { logEvent } from '../src/offline/httpHandler';

test('Should invoke the callback', () => {
  let called = false;
  let passedContent;
  logEvent({ body: { params: { name: 'Tester' } } }, null, (arg1, content) => {
    called = true;
    passedContent = content;
  });
  expect(called).toBeTruthy();
  expect(passedContent.statusCode).toBe(200);
  expect(passedContent.body).toEqual(JSON.stringify({
    status: "Success",
    body: {
      params: {
        name: "Tester"
      }
    }
  }));
});