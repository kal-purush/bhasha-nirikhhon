(Error as any).stackTraceLimit = Infinity;

import { JSDOM } from 'jsdom';

const dom = new JSDOM('<!doctype html><html><body></body></html>');
const window: any = dom.window;

global['document'] = window.document;
global['HTMLElement'] = window.HTMLElement;
global['XMLHttpRequest'] = window.XMLHttpRequest;
global['Node'] = window.Node;

import 'babel-polyfill';
import 'core-js/es7/reflect';
import 'web-animations-js';

/* The order of these imports is important, so tslint should shut up about it. */
/* tslint:disable:ordered-imports */
import 'zone.js/dist/zone';
import 'zone.js/dist/long-stack-trace-zone';
import 'zone.js/dist/proxy';
import 'zone.js/dist/sync-test';
import 'zone.js/dist/jasmine-patch';
import 'zone.js/dist/async-test';
import 'zone.js/dist/fake-async-test';
/* tslint:enable:ordered-imports */

import 'jquery';
import 'rxjs/Rx';