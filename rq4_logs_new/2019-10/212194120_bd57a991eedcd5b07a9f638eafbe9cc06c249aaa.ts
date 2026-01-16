import 'one-ui-styles/index.less';
import { Log } from 'one-log';
import { HAServer } from 'one-server-ha';
import { loader } from 'one-web-loader';
import { BrowserLogger } from 'one-web-logger';

import { init } from 'unch-app';

// set up the logger
const log = new Log([
    // browser console
    new BrowserLogger(),
]);

loader({
    log,
    url: 'settings.json',
}, async (options: any) => {
    const {
        settings,
        resolver,
    } = options;

    log.debug(
        'unch-app-web',
        'application settings', {
            settings
        }
    );

    // initialize the app 
    await init('app', {
        log,
        settings,
        resolver,
        server: new HAServer(
            settings['servers']
        )
    });
});