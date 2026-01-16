import { BaseHelper } from '@/baseHelper';
import { Page, expect } from '@playwright/test';
import { BreadCrumbHelper } from './breadCrumb.helper';

export class PageHelper extends BaseHelper {
    public breadcrumbHelper: BreadCrumbHelper;

    public constructor(page: Page) {
        super(page);
        this.breadcrumbHelper = new BreadCrumbHelper(page);
    }

    /**
     * Checks the page URL against the provided URL.
     *
     * @param {string} url - The URL to check against.
     * @return {Promise<void>} - A Promise that resolves when the check is complete.
     */
    public async checkPageUrl(url: string) {
        await expect(this._page, `Checking Page Url: ${url}`).toHaveURL(url);
    }

    /**
     * Checks the page title against the given title.
     *
     * @param {string} title - The title to compare against.
     * @return {Promise<void>} - A promise that resolves when the check is complete.
     */
    public async checkPageTitle(title: string) {
        await this.breadcrumbHelper.checkBreadCrumbTitle(title);
    }
}