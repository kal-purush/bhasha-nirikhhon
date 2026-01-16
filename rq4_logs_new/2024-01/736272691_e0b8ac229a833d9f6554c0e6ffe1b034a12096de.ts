import { $ } from "@wdio/globals"

export class WebViewScreen {

    private selectors = {

        webViewLabel: '//android.widget.TextView[@text="Webview"]',
        urlLabel: '//android.widget.TextView[@text="URL"]',
        urlInputField: '//android.widget.EditText[@content-desc="URL input field"]',
        urlInfoLabel: '//android.widget.TextView[@text="Enter an HTTPS url."]',
        goToSiteButton: '//android.view.ViewGroup[@content-desc="Go To Site button"]',

    }

    async getWebViewLabel() {
        return await $(this.selectors.webViewLabel);
    }

    async getUrlLabel() {
        return await $(this.selectors.urlLabel);
    }

    async getUrlInputField() {
        return await $(this.selectors.urlInputField);
    }

    async getUrlInfoLabel() {
        return await $(this.selectors.urlInfoLabel);
    }

    async goToSite() {
        return await $(this.selectors.goToSiteButton);
    }

    async enterUrl(url: string) {
        const urlInputFieldEle = $(this.selectors.urlInputField);
        (await urlInputFieldEle).waitForDisplayed();
        await urlInputFieldEle.setValue(url);
        await driver.hideKeyboard();
    }

    async clickGoToSiteButton() {
        const goToSiteButtonEle = await $(this.selectors.goToSiteButton);
        await goToSiteButtonEle.waitForDisplayed();
        await goToSiteButtonEle.click();
    }

}