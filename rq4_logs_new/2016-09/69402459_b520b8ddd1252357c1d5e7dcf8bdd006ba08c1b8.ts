/// <reference path="../../node_modules/@types/bluebird/index.d.ts" />

import * as Promise from "bluebird";

import { Crawler } from "./Crawler";

/**
 * A crawler for plain text resources.
 */
export abstract class TextCrawler extends Crawler<string> {
    /**
     * Loads a text resource.
     * 
     * @param contents   The raw resource.
     * @returns A Promise for the resource.
     */
    protected loadResource(contents: string): Promise<string> {
        return Promise.resolve(contents);
    }
}