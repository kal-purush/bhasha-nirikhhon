/// <reference path="../typings/phantom.d.ts" />

import { create as createPhantom, WebPage, PhantomJS } from "phantom";

import { IClub } from "./models/IClub";
import { IContact } from "./models/IContact";
import { ISchool } from "./models/ISchool";

export abstract class Crawler {
    /**
     * Collected landing pages that can be visited.
     */
    private landingPages: string[] = [];

    /**
     * Landing pages that have already been visited.
     */
    private visitedLandingPages: Set<string> = new Set<string>();

    /**
     * A school being populated.
     */
    private school: ISchool;

    /**
     * 
     */
    /* protected */ constructor(school: ISchool, landingPages: string[]) {
        this.school = school;
        this.landingPages.push(...landingPages);
    }

    /**
     * 
     */
    protected addLandingPage(landingPage: string): boolean {
        if (this.visitedLandingPages.has(landingPage)) {
            return false;
        }

        this.landingPages.push(landingPage);
        return true;
    }

    /**
     * 
     * 
     * @remarks In the future, this could probably merge duplicate contacts.
     */
    protected addContact(clubName: string, contact: IContact): void {
        this.getClub(clubName).contacts.push(contact);
    }

    /**
     * 
     */
    protected abstract crawlLandingPage(phantom: PhantomJS, webPage: WebPage, landingPage: string): Promise<void>;

    /**
     * 
     */
    private reset(): void {
        this.landingPages.length = 0;
        this.visitedLandingPages.clear();
    }

    /**
     * 
     */
    private getClub(clubName: string): IClub {
        if (this.school.clubs[clubName]) {
            return this.school.clubs[clubName];
        }

        return this.school.clubs[clubName] = {
            name: clubName,
            contacts: []
        };
    }

    /**
     * 
     */
    private crawl(): Promise<ISchool> {
        this.reset();

        return createPhantom()
            .then((phantom: PhantomJS): Promise<ISchool> => {
                let pendingWork: Promise<void> = Promise.resolve();

                for (let i: number = 0; i < this.landingPages.length; i += 1) {
                    const landingPage: string = this.landingPages[i];
                    this.visitedLandingPages.add(landingPage);

                    pendingWork = pendingWork
                        .then((): Promise<WebPage> => this.prepareLandingPage(phantom, landingPage))
                        .then((webPage: WebPage): Promise<void> => {
                            console.log(`Crawling ${landingPage}...`);
                            return this.crawlLandingPage(phantom, webPage, landingPage);
                        });
                }

                return pendingWork.then((): ISchool => this.school);
            });
    }

    /**
     * 
     */
    private prepareLandingPage(phantom: PhantomJS, landingPage: string): Promise<WebPage> {
        let webPage: WebPage;

        return phantom.createPage()
            .then((createdPage: WebPage): Promise<any> => {
                webPage = createdPage;
                return webPage.open(landingPage);
            })
            .then((): WebPage => webPage);
    }
}