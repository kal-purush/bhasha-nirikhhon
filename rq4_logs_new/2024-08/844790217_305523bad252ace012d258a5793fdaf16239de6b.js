import { expect } from "@playwright/test";

export class DashboardPage {
    urlDashboard = 'https://qa360.leal.co/dashboard/inicio'

    constructor(page) {
        this.page = page;
    }

    async checkIamDashboard(){
        const currentUrl = this.page.url()
        await expect(currentUrl).toBe(this.urlDashboard)
    }
}