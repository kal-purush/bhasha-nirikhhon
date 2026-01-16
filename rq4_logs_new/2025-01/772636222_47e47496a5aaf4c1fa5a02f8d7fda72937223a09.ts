import { test as base } from "./pageFixtures";
import { formUrl, journeyCreateUrl, mockRedirectUrl, noMessageText, noPaymentText, postSubmisionBtnLabel } from "../utils/constants";
import { JourneyTitlePage } from "../objects/journeyCreation/JourneyTitlePage";
import { JourneyFormPage } from "../objects/journeyCreation/JourneyFormPage";
import { JourneyPaymentPage } from "../objects/journeyCreation/JourneyPaymentPage";
import { JourneyMessagePage } from "../objects/journeyCreation/JourneyMessagePage";
import { JourneyPostSubmissionDetailsPage } from "../objects/journeyCreation/JourneyPostSubmissionDetailsPage";
import { ReviewPage } from "../objects/journeyCreation/ReviewPage";
import { DonePage } from "../objects/journeyCreation/DonePage";
import { createProviderAndPaymentRequest } from "../utils/payments";

type journeyFixtures = {
    basicJourney: {
        journeyLink: string;
        journeyName: string;
        journeyId: string;
    };
    journeyWithPayment: {
        journeyLink: string;
        journeyName: string;
        journeyId: string;
        paymentRequestName: string;
    };
};

export const test = base.extend<journeyFixtures>({
    basicJourney: async ({ publicServantPage }, use) => {
        const journeyName = `Test journey ${Date.now()}`;
        await publicServantPage.goto(journeyCreateUrl);

        const journeyTitlePage = new JourneyTitlePage(publicServantPage);
        await journeyTitlePage.enterName(journeyName);
        await journeyTitlePage.continue();

        const journeyFormUrlPage = new JourneyFormPage(publicServantPage);
        await journeyFormUrlPage.enterUrl(formUrl);
        await journeyFormUrlPage.continue();

        const journeyPaymentPage = new JourneyPaymentPage(publicServantPage);
        await journeyPaymentPage.choosePaymentRequest(noPaymentText);
        await journeyPaymentPage.continue();

        const journeyMessagePage = new JourneyMessagePage(publicServantPage);
        await journeyMessagePage.chooseMessage(noMessageText);
        await journeyMessagePage.continue();

        const journeyPostSubmissionPage = new JourneyPostSubmissionDetailsPage(publicServantPage);
        await journeyPostSubmissionPage.enterButtonLabel(postSubmisionBtnLabel)
        await journeyPostSubmissionPage.enterButtonLink(mockRedirectUrl);
        await journeyPostSubmissionPage.continue();

        const reviewPage = new ReviewPage(publicServantPage);
        await reviewPage.continue();

        const donePage = new DonePage(publicServantPage);
        const journeyLink = await donePage.getJourneyLink();
        const journeyId = await donePage.getJourneyId();

        await use({ journeyLink, journeyName, journeyId });
    },

    journeyWithPayment: async ({ publicServantPage, request }, use) => {
        const prName = `Test payment request ${Date.now()}`;
        await createProviderAndPaymentRequest(request, prName);

        const journeyName = `Test journey ${Date.now()}`;
        await publicServantPage.goto(journeyCreateUrl);

        const journeyTitlePage = new JourneyTitlePage(publicServantPage);
        await journeyTitlePage.enterName(journeyName);
        await journeyTitlePage.continue();

        const journeyFormUrlPage = new JourneyFormPage(publicServantPage);
        await journeyFormUrlPage.enterUrl(formUrl);
        await journeyFormUrlPage.continue();

        const journeyPaymentPage = new JourneyPaymentPage(publicServantPage);
        await journeyPaymentPage.choosePaymentRequest(prName);
        await journeyPaymentPage.continue();

        const journeyMessagePage = new JourneyMessagePage(publicServantPage);
        await journeyMessagePage.chooseMessage(noMessageText);
        await journeyMessagePage.continue();

        const journeyPostSubmissionPage = new JourneyPostSubmissionDetailsPage(publicServantPage);
        await journeyPostSubmissionPage.enterButtonLabel(postSubmisionBtnLabel)
        await journeyPostSubmissionPage.enterButtonLink(mockRedirectUrl);
        await journeyPostSubmissionPage.continue();

        await new ReviewPage(publicServantPage).continue();

        const donePage = new DonePage(publicServantPage);
        const journeyLink = await donePage.getJourneyLink();
        const journeyId = await donePage.getJourneyId();

        await use({ journeyLink, journeyName, journeyId, paymentRequestName: prName });
    },
});