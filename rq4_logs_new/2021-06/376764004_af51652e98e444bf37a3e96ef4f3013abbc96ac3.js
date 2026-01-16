/**
 * Require tools
 */
const { app, Menu, Tray } = require('electron')
const autolaunch = require('./utils/autolaunch');
const path = require('path');
const { exec } = require('child_process');

const Logger = require('./utils/logger');
const log = Logger.getLogger();

const binariesPath = require('./utils/binaries');


/**
 * Don't show app in the dock
 */
app.dock.hide();

/**
 * Define global vars
 */
let tray;

/**
 *
 */
const createTray = () => {
    tray = new Tray(path.join(__dirname, 'assets', 'tray-icon-Template.png'));

    const pathToBinary = path.join(binariesPath, 'osx-cpu-temp')

    setInterval(() => {
        exec(pathToBinary.replaceAll(' ', '\\ '), (exception, stdout, stderr) => {
            if (exception) {
                log.error(exception);
            }

            if (stderr) {
                log.error(stderr);
            }

            /**
             * Remove line break
             * @type {string}
             */
            let response = stdout.trim();

            /**
             * Remove " °C"
             * @type {string}
             */
            let temp = response.slice(0, -3);

            tray.setTitle(`${temp}°`)
        })
    }, 1000)


    tray.setContextMenu(Menu.buildFromTemplate([
        {
            label: 'Open at Login',
            type: 'checkbox',
            checked: app.getLoginItemSettings().openAtLogin,
            click: autolaunch.toggle,
        },
        { type: 'separator' },
        {
            label: `Quit`,
            role: 'quit'
        }
    ]))
};


/**
 * On ready initial function
 */
app.on('ready', () => {
    try {
        log.info('App is ready');

        /**
         * Prepare tray icon
         */
        createTray();

        /**
         * Do not try to check for updates in dev mode
         */
        if (!require('electron-is-dev')) {
            require('./utils/autoupdater');
        }
    } catch (error) {
        log.error(error);

        app.quit();
    }
});

/**
 * Catch runtime exceptions and rethrow them to "app's onready catch"
 */
process.on("uncaughtException", (err) => {
    log.error('uncaughtException');
    log.error(err);

    throw err;
});

/**
 * do we need this ?????
 */
process.on("unhandledRejection", (err) => {
    log.error('unhandledRejection');
    log.error(err);

    throw err;
});