import { AppPage } from './app.po';
import { browser, logging } from 'protractor';

import * as faker from "faker/locale/pt_BR";

describe('Sales Page', () => {
    let page: AppPage;

    beforeEach(() => {
        page = new AppPage();
    });

    describe('when not logged in', () => {
        it('should display Login', () => {
            page.navigateToBase();

            page.navTo('/sales');

            expect(page.getTitleText()).toEqual('Login');
        });
    });

    describe('when is logged in', () => {
        const code = faker.random.alphaNumeric(6).replace(' ', 'W');

        beforeAll(() => {
            page.logIn('special2@gg.com', 'xpto9999');
        });

        it('should display Sales', () => {
            page.navTo('/sales');

            expect(page.getTitleText()).toEqual('SALES');
        });

        it('should have sales', () => {
            page.navTo('/sales');

            expect(page.getTableRowCount()).toBeGreaterThan(0);
        });

        it('should create sale', () => {
            page.navTo('/sales');

            page.clickCreateButton();

            page.submitForm({
                cpf: '71188126253',
                code,
                value: 1250.35,
                data: '02/03/2020 15:00:04'
            });

            expect(page.toastSuccessMessage()).toEqual('Sale created');
        });

        it('should edit sale', () => {
            page.navTo('/sales');

            page.clickEditButton(code);

            page.submitForm({});

            expect(page.toastSuccessMessage()).toEqual('Sale updated');
        });

        it('should delete sale', () => {
            page.navTo('/sales');

            page.clickDeleteButton(code);

            page.clickConfirmButton();

            expect(page.toastSuccessMessage()).toEqual('Sale removed');
        });
    });

    afterEach(async () => {
        // Assert that there are no errors emitted from the browser
        const logs = await browser.manage().logs().get(logging.Type.BROWSER);
        expect(logs).not.toContain(jasmine.objectContaining({
            level: logging.Level.SEVERE,
        } as logging.Entry));
    });
});