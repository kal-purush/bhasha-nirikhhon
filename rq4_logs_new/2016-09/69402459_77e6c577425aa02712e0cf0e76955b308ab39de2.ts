/// <reference path="../../../node_modules/@types/bluebird/index.d.ts" />
/// <reference path="../../../node_modules/@types/cheerio/index.d.ts" />

import * as Promise from "bluebird";
import "cheerio";

import { IOrganization } from "../../models/IOrganization";
import { JsonCrawler } from "../JsonCrawler";
import { TextCrawler } from "../TextCrawler";

/**
 * An initial response from the OrgSync orgs page.
 */
interface IOrgSyncResponse {
    /**
     * Descriptors of ASU organizations.
     */
    data: IOrgSyncDescriptor[];
}

/**
 * A description of an ASU organization.
 */
interface IOrgSyncDescriptor {
    /**
     * The name of the organization.
     */
    name: string;

    /**
     * Links to organiation resources.
     */
    links: {
        /**
         * The web URL for the organization.
         */
        web: string;
    };
}

/**
 * Crawls an OrgSync website for organizations.
 */
export class OrgSyncCrawler extends JsonCrawler<IOrgSyncResponse> {
    /**
     * Initializes a new instance of the ASUCrawler class.
     * @param url   The url to fetch list of orgs e.g. https://api.orgsync.com/api/v3/communities/334/portals?key=wI3OYsSEiIuWn5UbKuNKz2LgTLAsbpdIsi9lHGr0R2E&all=true
     */
    public constructor(url: string) {
        super("ASU");

        this.addResource({
            callback: this.crawlOrganizationsResponse,
            url: url
        });
    }

    /**
     * Crawls the page of all organizations.
     */
    private crawlOrganizationsResponse(response: IOrgSyncResponse, url: string): Promise<void> {
        const crawls: Promise<void>[] = response.data
            .map((descriptor: IOrgSyncDescriptor): Promise<void> => {
                return new OrgSyncOrganizationCrawler(descriptor.links.web + "/display_profile", descriptor.name)
                    .crawl()
                    .then((results: IOrganization): void => {
                        this.addOrganization(results);
                    });
            });

        return Promise.all(crawls) as Promise<any>;
    }
}

/**
 * Crawls a single ASU organization's API response.
 */
export class OrgSyncOrganizationCrawler extends TextCrawler {

    private organizationName: string;

    /**
     * Initializes a new instance of the ASUOrganizationCrawler class.
     * 
     * @param url   The organization's API URL.
     */
    constructor(url: string, name: string) {
        super(url);
        this.organizationName = name;

        this.addResource(
            {
                callback: this.crawlOrganizationResponse,
                options: {
                    headers: {
                        /* tslint:disable max-line-length */
                        "Accept": "*/*;q=0.5, text/javascript, application/javascript, application/ecmascript, application/x-ecmascript",
                        "Accept-Encoding": "gzip, deflate, sdch, br",
                        "Accept-Language": "en-US,en;q=0.8",
                        "Cache-Control": "no-cache",
                        "Connection": "keep-alive",
                        "Cookie": "my_school=arizona-state-university; __utma=64252785.1797029133.1475088512.1475088512.1475088512.2; __utmb=64252785.8.10.1475088512; __utmc=64252785; __utmz=64252785.1475088512.1.1.utmcsr=asu.orgsync.com|utmccn=(referral)|utmcmd=referral|utmcct=/search; _os_session=230f640f83f11c67a635b4f6ea56188a",
                        "DNT": "1",
                        "Host": "orgsync.com",
                        "Pragma": "no-cache",
                        "Referer": "https://orgsync.com/115401/chapter",
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36",
                        "X-CSRF-Token": "uBqhtROPQhbvP6FuIfvfENU6HcEeB0edeUkOxeGMtU4=",
                        "X-NewRelic-ID": "VgQHVFBACQsHXFZU",
                        "X-Requested-With": "XMLHttpRequest"
                        /* tslint:enable max-line-length */
                    },
                    url
                },
                url
            });
    }

    /**
     * Crawls an organization's API response.
     */
    private crawlOrganizationResponse(text: string, url: string): Promise<void> {
        const emails: string[] = text.match(/mailto\:.*\"/gi);

        if (!emails) {
            return Promise.resolve();
        }

        const email: string = emails[0]
            .split("\"")[0]
            .replace("mailto:", "")
            .replace("\"", "")
            .replace("\\", "")
            .replace(">", "");

        this.addContact(this.organizationName, { email });

        return Promise.resolve();
    }
}