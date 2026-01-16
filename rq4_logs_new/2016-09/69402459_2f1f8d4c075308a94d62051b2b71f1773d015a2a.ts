/// <reference path="../../typings/phantom.d.ts" />

import { Crawler } from "../crawler";
import { PhantomJS, WebPage } from "phantom";

export class LinkedInCrawler extends Crawler {
    /**
     * Initializes a new instance of the LinkedInCrawler class.
     */
    public constructor(search: string) {
        super(
            "LinkedIn",
            [
                "http://union.rpi.edu/clubs"
            ]);
    }

    /**
     * Crawls an RPI webpage
     */
    protected crawlLandingPage(webPage: WebPage, landingPage: string): Promise<void> {
        // idea: continuously scroll down until the page height doesn't increase
        return Promise.resolve();
    }
}