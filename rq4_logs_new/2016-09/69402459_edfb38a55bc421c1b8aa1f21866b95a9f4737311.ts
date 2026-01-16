/// <reference path="../../node_modules/@types/cheerio/index.d.ts" />

import * as Promise from "bluebird";
import "cheerio";

import { Crawler, ILandingPageCallback } from "../crawler";
import { IContact } from "../models/IContact";

interface ILinkAttribs {
    href: string;
}

export class MITSloanCrawler extends Crawler {
    /**
     * Initializes a new instance of the MITSloanCrawler class.
     */
    public constructor() {
        super("MIT Sloan");

        this.addLandingPage("http://mitsloan.mit.edu/student-life/clubs/", this.crawlOrganizationsPage as ILandingPageCallback<this>);
    }

    /**
     * Crawls the page of all organizations.
     */
    private crawlOrganizationsPage($: CheerioStatic, url: string): Promise<void> {
        const contacts: IContact[] = [];

        $("table.MITTable tr")
            .each(function (i: number, rowElement: CheerioElement): void {
                const cells: Cheerio = $(this).find("td");

                const name: string = cells.first().text();
                const email: string = cells.last().text();

                contacts.push({ name, email });
            });

        for (const contact of contacts) {
            this.addContact(contact.name, contact.email);
        }

        return Promise.resolve();
        // idea: continuously scroll down until the page height doesn't increase
    }
}