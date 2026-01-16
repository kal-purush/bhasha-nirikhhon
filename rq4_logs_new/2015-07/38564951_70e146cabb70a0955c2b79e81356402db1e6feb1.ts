
/// <reference path='../../typings/node/node.d.ts'/>
/// <reference path='../../typings/mkdirp/mkdirp.d.ts'/>
/// <reference path='../../typings/browserstack-webdriver/browserstack-webdriver.d.ts'/>

import webdriver = require('browserstack-webdriver');
import cf from '../../conf/conf';
import sys from '../sys';
import * as fs from 'fs';
import { dirname } from 'path';
import { sync as createFolder } from 'mkdirp';

export class WebdriverTest {
    private browserstackUser: string;
    private browserstackKey: string;
    private driver: webdriver.Driver;
    private server: string;
    private currentControlFlow: Promise<any>;
    private pendingEvaluation: (evaluation: (...args: any[]) => Promise<any>) => void;

    constructor(public capabilites: webdriver.Capabilities) {
        this.capabilites['browserstack.user'] = this.capabilites['browserstack.user'] ||
            process.env.BROWSERSTACK_USER ||
            cf.DEFAULT_BROWSERSTACK_USER;

        this.capabilites['browserstack.key'] = this.capabilites['browserstack.key'] ||
            process.env.BROWSERSTACK_KEY ||
            cf.DEFAULT_BROWSERSTACK_KEY;

        this.capabilites['browserstack.local'] = 'true';

        this.server = process.env.SELENIUM_SERVER || cf.DEFAULT_SELENIUM_SERVER;

        this.driver = new webdriver.Builder()
            .usingServer(this.server)
            .withCapabilities(this.capabilites)
            .build();
    }

    public get(url: string): WebdriverTest {
        this.currentControlFlow = this.driver.get(url);

        return this;
    }

    public click(element: webdriver.Hash | string): WebdriverTest {
        this.currentControlFlow = this.currentControlFlow.then(() => {
            if (typeof element === 'string') {
                return this.driver.findElement(webdriver.By.id(element)).click();
            }
            else {
                return this.driver.findElement(element).click();
            }
        });

        return this;
    }

    public input(element: webdriver.Hash | string, keys: string): WebdriverTest {
        this.currentControlFlow = this.currentControlFlow.then(() => {
            if (typeof element === 'string') {
                return this.driver.findElement(webdriver.By.id(element)).click();
            }
            else {
                return this.driver.findElement(element).sendKeys(keys);
            }
        });

        return this;
    }

    public wait(element: webdriver.Hash | string): WebdriverTest {
        this.currentControlFlow = this.currentControlFlow.then(() => {
            this.driver.wait(() => {
                if (typeof element === 'string') {
                    return this.driver.isElementPresent(webdriver.By.id(element)).then(function() {
                        return true
                    });
                }
                else {
                    return this.driver.isElementPresent(element).then(function() {
                        return true
                    });
                }
            }, cf.WEBDRIVER_IDLE_TIME);
        });

        return this;
    }

    public screenshot(filePath: string): WebdriverTest {
        this.currentControlFlow = this.currentControlFlow.then(() => {
            let promise = new Promise<void>((resolve, reject)=>{
                this.driver.takeScreenshot().then(data => {
                    createFolder(dirname(filePath));
                    fs.writeFile(filePath, data.replace(/^data:image\/png;base64,/,''), 'base64', function(err) {
                        if(err) reject(err);
                    });
                    resolve();
                }).then(null, (err: Error) => {
                    if(err) reject(err);
                });
            });

            return promise;
        });

        return this;
    }

    public end(callback?: (err?: Error) => void): void {
        this.currentControlFlow.then(() => {
            this.driver.quit().then(() => {
                if (callback) {
                    callback();
                }
            });
        });
    }
}