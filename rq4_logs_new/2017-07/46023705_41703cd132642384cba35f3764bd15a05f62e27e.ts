(Error as any).stackTraceLimit = Infinity;

import 'babel-polyfill';
import 'reflect-metadata';

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

import { TestBed } from '@angular/core/testing';
import { BrowserTestingModule, platformBrowserTesting } from '@angular/platform-browser/testing';

TestBed.initTestEnvironment(
    BrowserTestingModule,
    platformBrowserTesting()
);