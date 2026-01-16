// Use browser to access other sites (that are running angular)
import { element, by } from 'protractor';

// Use SkyHostBrowser to access your locally served SPA
import { SkyHostBrowser } from '@blackbaud/skyux-builder/runtime/testing/e2e';

describe('Home Component', () => {

  // done is optional callback if you're doing async tests
  it('should render', (done) => {

    // Host URL + SPA Name automatically included
    SkyHostBrowser.get('/');
    element(by.tagName('h1')).getText().then((h1result: string) => {
      expect(h1result).toEqual('SKY UX Template');

      element(by.className('sky-alert')).getText().then((alertResult: string) => {
        expect(alertResult).toEqual(
          `You've just taken your first step into a larger world.`
        );
        // Since we've said this is an async test, we need to call done
        done();
      });
    });

  });
});