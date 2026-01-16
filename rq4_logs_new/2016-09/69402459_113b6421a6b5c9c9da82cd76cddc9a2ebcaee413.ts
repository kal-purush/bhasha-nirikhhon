/// <reference path="../../node_modules/@types/cheerio/index.d.ts" />

import * as Promise from "bluebird";
import * as cheerio from "cheerio";

import { Crawler, ILandingPageCallback } from "../crawler";
// import { IContact } from "../models/IContact";

interface ILinkAttribs {
    href: string;
}

export class StonyBrookCrawler extends Crawler {
    /**
     * Initializes a new instance of the StonyBrookCrawler class.
     */
    public constructor() {
        super("SUNY Stony Brook");

        this.addLandingPage("https://stonybrook.collegiatelink.net/organizations", this.crawlOrganizationsPage as ILandingPageCallback<this>);
    }

    /**
     * Crawls the page of all organizations.
     */
    private crawlOrganizationsPage($: CheerioStatic, url: string): Promise<void> {
        $("#results .result h5 a")
            .each((i: number, element: CheerioElement): void => {
                const href: string = (element.attribs as ILinkAttribs).href;
                const organizationUrl: string = "https://stonybrook.collegiatelink.net/organization" + href;

                this.addLandingPage(organizationUrl, this.crawlOrganizationPage as ILandingPageCallback<this>);
            });

        return Promise.resolve();
        // idea: continuously scroll down until the page height doesn't increase
    }

    /**
     * 
     */
    private crawlOrganizationPage($: CheerioStatic, url: string): Promise<void> {
        $(".container-orgcontact h5")
            .each((i: number, element: CheerioElement): void => {
                // const name: string = element.content();

                // this.addContact(url, { name });
            });

        return Promise.resolve();
    }
}