
/// <reference path='../es6-promise/es6-promise.d.ts'/>

declare module 'selenium-webdriver' {

    export interface Hash {
        id?: string;
        className?: string;
        js?: string;
        linkText?: string;
        name?: string;
        partialLinkText?: string;
        tagName?: string;
        xpath?: string;
    }

    export interface Capabilities {
        [capability: string]: any;
    }

    export interface Element {

    }

    class Locator {}

    export type ElementQuery<T> = Hash | Locator | Condition<T>;

    class WebElement {
        public click(): Promise<void>;
        public sendKeys(keys: string): Promise<void>;
    }

    interface Cookie {
        name: string;
        value: string;
        path?: string;
        domain?: string;
        secure?: boolean;
        httpOnly?: boolean;

        /**
         * When the cookie expires, specified in seconds since midnight, January 1, 1970 UTC.
         */
        expiry?: number;
    }

    interface Log {
        timestamp: number;
        level: string;
        message: string;
    }

    interface Position {
        x: number;
        y: number;
    }

    interface Size {
        width: number;
        height: number;
    }

    class Window {
        getPosition(): Promise<Position>;
        setPosition(x: number, y: number): Promise<void>;
        getSize(): Promise<Size>;
        setSize(width: number, height: number): Promise<void>;
        maximize(): Promise<void>;
    }

    class Options {
        public addCookie(name: string, value: string, path?: string, domain?: string, isSecure?: boolean, expiry?: number): Promise<void>;
        public deleteCookie(name: string): Promise<void>;
        public deleteAllCookies(): Promise<void>;
        public getCookie(name: string): Promise<Cookie>;
        public getCookies(): Promise<Cookie[]>;
        public window(): Window;
    }

    class Driver {
        public get(url: string): Promise<void>;
        public getTitle(): Promise<string>;
        public takeScreenshot(): Promise<string>;
        public findElement<T>(query: ElementQuery<T>): WebElement;
        public isElementPresent<T>(query: ElementQuery<T>): Promise<void>;
        public quit(): Promise<void>;
        public manage(): Options;
        public wait<T>(condition: Condition<T>, timeout?: number, message?: string): Promise<void>;
    }

    class Alert {

    }

    class Condition<T> {
        constructor(message: string, fn: (...arg: any[]) => any);
        description(): string;
    }

    export class Builder {
        public usingServer(url: string): Builder;
        public withCapabilities(capabilities: Capabilities): Builder;
        public build(): Driver;
    }

    class _By {
        name: (name: string) => Locator;
        id: (id: string) => Locator;
        className: (className: string) => Locator;
        css: (css: string) => Locator;
        linkText: (text: string) => Locator;
        js: (script: string, args: any) => Promise<any>;
        partialLinkText: (text: string) => Locator;
        tagName: (tagName: string) => Locator;
        xpath: (xpath: string) => Locator;
    }

    export var By: _By;

    class _Until {
        alertIsPresent(): Condition<Alert>;
        elementIsDisabled(element: WebElement): Condition<boolean>;
        elementIsEnabled(element: WebElement): Condition<boolean>;
        elementIsNotSelected(element: WebElement): Condition<boolean>;
        elementIsNotVisible(element: WebElement): Condition<boolean>;
        elementIsSelected(element: WebElement): Condition<boolean>;
        elementIsVisible(element: WebElement): Condition<boolean>;
        elementLocated(element: Locator | Hash): Condition<WebElement>;
        elementTextContains(element: WebElement, substr: string): Condition<boolean>;
        elementTextIs(element: WebElement, text: string): Condition<boolean>;
        elementTextMatches(element: WebElement, regex: RegExp): Condition<boolean>;
        elementsLocated(element: Locator): Condition<WebElement[]>;
        stalenessOf(element: WebElement): Condition<boolean>;
        titleContains(substr: string): Condition<boolean>;
        titleIs(title: string): Condition<boolean>;
        titleMatches(regex: RegExp): Condition<boolean>;
    }

    export var until: _Until;
}