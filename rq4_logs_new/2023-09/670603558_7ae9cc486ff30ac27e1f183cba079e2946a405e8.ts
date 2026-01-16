import { BaseHelper } from '@/baseHelper';
import { expect } from '@playwright/test';

/**
 * Helper class for BreadCrumbs
 */
export class BreadCrumbHelper extends BaseHelper {
    public getBreadCrumbContainer() {
        return this.locate('div.breadcrumbs');
    }

    public async getBreadCrumbTitle() {
        const container = this.getBreadCrumbContainer().getLocator();
        const titleTexts = await container.locator('> h1').allInnerTexts();

        if (titleTexts.length < 1) return null;
        return titleTexts[0];
    }

    public async checkBreadCrumbTitle(title: string): Promise<void> {
        const breadCrumbTitle = await this.getBreadCrumbTitle();

        expect(breadCrumbTitle, {
            message: `Breadcrumb title check: "${title}"`,
        }).toBe(title);
    }
}