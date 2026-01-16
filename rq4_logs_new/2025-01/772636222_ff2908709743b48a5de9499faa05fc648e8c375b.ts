import { test as base } from "./journeyFixture";
import { citizens, mockPassword, myGovIdMockSettings } from "../utils/constants";
import { FormPage } from "../objects/journeyExecution/FormPage";
import { PostSubmissionPage } from "../objects/journeyExecution/PostSubmissionPage";
import { MyGovIdMockLoginPage } from "../objects/MyGovIdMockLoginPage";
import { PayPage } from "../objects/journeyExecution/PayPage";
import { ManualBankTransferTransactionPage } from "../objects/journeyExecution/ManualBankTransferTransactionPage";

type submissionFixtures = {
    basicSubmission: {
        userEmail: string;
        journeyName: string;
        journeyId: string;
    };
    submissionWithPayment: {
        userEmail: string;
        journeyName: string;
        journeyId: string;
    };
};

const [name, surname] = citizens[0].split(" ");
const userEmail = `${name.toLocaleLowerCase()}.${surname.toLocaleLowerCase()}@${myGovIdMockSettings.citizenEmailDomain}`;

export const test = base.extend<submissionFixtures>({
    basicSubmission: async ({ citizenPage, basicJourney }, use) => {
        await citizenPage.goto(basicJourney.journeyLink);

        const formPage = new FormPage(citizenPage);
        await formPage.fillInput("test");
        await formPage.submit();

        await new PostSubmissionPage(citizenPage).continue();

        await use({ userEmail, journeyName: basicJourney.journeyName, journeyId: basicJourney.journeyId });
    },

    submissionWithPayment: async ({ citizenPage, journeyWithPayment }, use) => {
        await citizenPage.goto(journeyWithPayment.journeyLink);

        const formPage = new FormPage(citizenPage);
        await formPage.fillInput("test");
        await formPage.submit();

        await citizenPage.waitForLoadState();

        const logtoLoginBtn = await citizenPage.getByRole("button", {
            name: "Continue with MyGovId",
        });
        await logtoLoginBtn.click();

        const loginPage = new MyGovIdMockLoginPage(citizenPage);

        await loginPage.selectUser(citizens[0], "citizen");
        await loginPage.enterPassword(mockPassword);
        await loginPage.submitLogin(citizens[0]);

        const payPage = new PayPage(citizenPage);
        await payPage.selectPaymentMethod("banktransfer");
        await payPage.proceedToPayment();

        const bankTrasferPage = new ManualBankTransferTransactionPage(citizenPage);
        await bankTrasferPage.confirmPayment();

        await new PostSubmissionPage(citizenPage).continue();

        await use({ userEmail, journeyName: journeyWithPayment.journeyName, journeyId: journeyWithPayment.journeyId });
    }
});