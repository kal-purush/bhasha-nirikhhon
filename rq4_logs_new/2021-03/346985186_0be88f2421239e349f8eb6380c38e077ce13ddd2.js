// src/helpers/setCookies.js

export default function setCookies(config) {
    if (config.acceptanalytics) {
        // eslint-disable-next-line
        gtag('config', 'G-CLDSCPRCYE');
    }
}