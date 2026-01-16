import { Locator } from '@playwright/test';
import { DialogHelper } from './dialog.helper';

export class PortalSelectorHelper extends DialogHelper {
    /**
     * Retrieves the portal selector container.
     *
     * @return {PortalSelectorHelper} The dialog container.
     */
    public getPortalSelectorContainer(): PortalSelectorHelper {
        return this.getDialogContainer();
    }

    /**
     * Retrieves the portal element with the given portal name.
     *
     * @param {string} portalName - The name of the portal.
     * @return {Promise<Locator>} The portal element.
     */
    public async getPortalElement(portalName: string): Promise<Locator> {
        const container = this.getPortalSelectorContainer();
        return container
            .getLocator()
            .locator('//div[contains(@class, "productcard")]')
            .getByText(portalName)
            .locator('//ancestor::div[contains(@class, "productcard")]');
    }

    /**
     * Retrieves the FinOps Portal element.
     *
     * @return {Promise<Locator>} The FinOps Portal element.
     */
    public getFinopsPortal() {
        return this.getPortalElement('FinOps Portal');
    }

    /**
     * Selects the Finops Portal.
     *
     * @return {Promise<void>} Does not return anything.
     */
    public async selectFinopsPortal(): Promise<void> {
        (await this.getFinopsPortal()).click();
    }

    /**
     * Retrieves the Recko Portal element.
     *
     * @return {Promise<Locator>} The Recko Portal element.
     */
    public getReckoPortal(): Promise<Locator> {
        return this.getPortalElement('Reco Portal');
    }

    /**
     * Selects the Recko portal asynchronously.
     *
     * @return {Promise<void>} Promise that resolves when the Recko portal is selected.
     */
    public async selectReckoPortal(): Promise<void> {
        (await this.getReckoPortal()).click();
    }

    /**
     * Retrieves the employee portal element from the page.
     *
     * @return {Promise<Locator>} A promise that resolves to the employee portal element locator.
     */
    public getEmployeePortal(): Promise<Locator> {
        return this.getPortalElement('Employee Portal');
    }

    /**
     * Selects the employee portal asynchronously.
     *
     * @return {Promise<void>} A promise that resolves when the employee portal is selected.
     */
    public async selectEmployeePortal(): Promise<void> {
        (await this.getEmployeePortal()).click();
    }

    /**
     * Retrieves the vendor portal element from the page.
     *
     * @return {Promise<Locator>} A promise that resolves to the vendor portal element.
     */
    public getVendorPortal(): Promise<Locator> {
        return this.getPortalElement('Vendor Portal');
    }

    /**
     * Selects the vendor portal asynchronously.
     *
     * @return {Promise<void>} - A promise that resolves to void.
     */
    public async selectVendorPortal(): Promise<void> {
        (await this.getVendorPortal()).click();
    }
}