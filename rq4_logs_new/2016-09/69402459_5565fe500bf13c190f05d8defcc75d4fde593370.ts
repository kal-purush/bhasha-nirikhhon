/// <reference path="../../node_modules/@types/cheerio/index.d.ts" />

import * as Promise from "bluebird";
import "cheerio";

import { Crawler, ILandingPageCallback } from "../crawler";
import { IContact } from "../models/IContact";

interface ILinkAttribs {
    href: string;
}

export class StRoseCrawler extends Crawler {
    /**
     * Initializes a new instance of the StRoseCrawler class.
     */
    public constructor() {
        super("MIT Sloan");

        this.addLandingPage(
            "https://www.strose.edu/student-life/leadership-opportunities/student-association/student-association-clubs/",
            this.crawlOrganizationsPage as ILandingPageCallback<this>);
    }

    /**
     * Crawls the page of all organizations.
     */
    private crawlOrganizationsPage($: CheerioStatic, url: string): Promise<void> {
        const contacts: IContact[] = [];

        $(".typography p")
            .each(function (i: number, rowElement: CheerioElement): void {
                const name: string = $(this).find("strong").text();
                const email: string = $(this).find("a").text();

                contacts.push({ name, email });
            });

        for (const contact of contacts) {
            this.addContact(contact.name, contact.email);
        }

        return Promise.resolve();
    }
}