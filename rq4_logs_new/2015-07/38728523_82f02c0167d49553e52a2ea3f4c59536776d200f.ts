export class caculatorPage {
    elFirst = element(by.model('first'));
    elSecond = element(by.model('second'));
    btnGo = element(by.id('gobutton'));
    elLatest = element(by.binding('latest'));

    open() {
        browser.get('http://juliemr.github.io/protractor-demo/');
    }

    caculate(first, second) {
        this.elFirst.sendKeys(first);
        this.elSecond.sendKeys(second);
        this.btnGo.click();
    }


}