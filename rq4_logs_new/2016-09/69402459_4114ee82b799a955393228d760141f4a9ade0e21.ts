/// <reference path="../../../node_modules/@types/bluebird/index.d.ts" />
/// <reference path="../../../node_modules/@types/cheerio/index.d.ts" />

import * as Promise from "bluebird";
import "cheerio";

import { IResourceDescriptor } from "../Crawler";
import { WebPageCrawler } from "../WebPageCrawler";

export class OhioStateCrawler extends WebPageCrawler {
    /**
     * Initializes a new instance of the OhioStateCrawler class.
     */
    public constructor() {
        super("Ohio State Sloan");

        this.addResource({
            callback: this.crawlOrganizationsPage,
            url: "http://activities.osu.edu/involvement/student_organizations/find_a_student_org?v=list&l=ALL",
        });
    }

    /**
     * Crawls the page of all organizations.
     */
    private crawlOrganizationsPage($: CheerioStatic, resource: IResourceDescriptor<CheerioStatic>): Promise<void> {
        const organizations: { name: string, url: string }[] = [];

        $("tr td strong a")
            .each(function (i: number, rowElement: CheerioElement): void {
                organizations.push({
                    name: $(this).text().trim(),
                    url: (rowElement.attribs as any).href
                });
            });

        for (const organization of organizations) {
            this.addResource({
                callback: this.crawlOrganizationPage,
                organization: name,
                url: "http://activities.osu.edu" + organization.url
            });
        }

        return Promise.resolve();
    }

    /**
     * Crawls the page of a single organization.
     */
    private crawlOrganizationPage($: CheerioStatic, resource: IResourceDescriptor<CheerioStatic>): Promise<void> {
        const leaderElement: Cheerio = $(`table tr a[href^="mailto:"]`).first();
        const leaderEmail: string = leaderElement.attr("href").replace("mailto:", "");
        const leaderName: string = $(leaderElement).parent("tr").find("th").text();

        this.addContact(
            resource.organization,
            {
                name: leaderName,
                email: leaderEmail
            });

        return Promise.resolve();
    }
}