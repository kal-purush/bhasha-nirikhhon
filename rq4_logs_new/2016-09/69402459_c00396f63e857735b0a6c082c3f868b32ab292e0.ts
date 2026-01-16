/// <reference path="../../../node_modules/@types/bluebird/index.d.ts" />

import * as Promise from "bluebird";

import { IOrganization } from "../../models/IOrganization";
import { JsonCrawler } from "../JsonCrawler";

/**
 * Response data for Brigham Young's organiations.
 */
type IBrighamYoungData = { name: string }[];

/**
 * A partial API response for a Brigham Young organization.
 */
interface IBrighamYoungOrganization {
    /**
     * Important members of the organization.
     */
    members: IBrighamYoungOrganizationMember[];

    /**
     * The name of the organization.
     */
    name: string;

    /**
     * The organization URL used for detailed API calls.
     */
    slug: string;
}

/**
 * A member summary of a Brigham Young organization leader.
 */
interface IBrighamYoungOrganizationMember {
    /**
     * The backing user data.
     */
    user: IBrighamYoungOrganizationUser;
}

/**
 * Backing user data for a Brigham Young organization leader.
 */
interface IBrighamYoungOrganizationUser {
    /**
     * The user's name.
     */
    name: string;

    /**
     * The user's email.
     */
    email: string;
}

/**
 * Crawls an OrgSync website for organizations.
 */
export class BrighamYoungCrawler extends JsonCrawler<IBrighamYoungData> {
    /**
     * Initializes a new instance of the BrighamYoungCrawler class.
     */
    public constructor() {
        super("Brigham Young");

        this.addResource({
            url: "https://clubs.byu.edu/api/clubs",
            callback: this.crawlOrganizationsResponse
        });
    }

    /**
     * Crawls the API response of all organizations.
     */
    private crawlOrganizationsResponse(response: IBrighamYoungData): Promise<void> {
        const crawls: Promise<void>[] = response
            .map((organization: IBrighamYoungOrganization): Promise<void> => {
                return new BrighamYoungOrganizationCrawler(organization)
                    .crawl()
                    .then((results: IOrganization): void => {
                        this.addOrganization(results);
                    });
            });

        return Promise.all(crawls) as Promise<any>;
    }
}

/**
 * Crawls a single Brigham Young organization API response.
 */
class BrighamYoungOrganizationCrawler extends JsonCrawler<IBrighamYoungOrganization> {
    /**
     * Initializes a new instance of the BrighamYoungOrganizationCrawler class.
     */
    public constructor(organization: IBrighamYoungOrganization) {
        super("Brigham Young " + organization.name);

        this.addResource({
            url: "https://clubs.byu.edu/api/clubs/" + organization.slug,
            callback: this.crawlOrganizationResponse
        });
    }

    /**
     * Crawls the API response of an organization.
     */
    private crawlOrganizationResponse(response: IBrighamYoungOrganization): Promise<void> {
        const name: string = response.name;
        const email: string = this.getUserEmail(response.members);

        if (email) {
            this.addContact(name, { email });
        }

        return Promise.resolve();
    }

    /**
     * Finds the first user email from members.
     * 
     * @param members   Member summary of organization leaders.
     * @returns The first user email, if found.
     */
    private getUserEmail(members: IBrighamYoungOrganizationMember[]): string | undefined {
        for (const member of members) {
            const user: IBrighamYoungOrganizationUser = member.user;
            if (!user) {
                continue;
            }

            if (user.email) {
                return user.email;
            }
        }

        return undefined;
    }
}