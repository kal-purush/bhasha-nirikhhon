import ElementArrayFinder = protractor.ElementArrayFinder;
import ElementFinder = protractor.ElementFinder;
import Promise = webdriver.promise.Promise;

export class CounterPageObject {

	private increaseButton: ElementFinder;
	private decreaseButton: ElementFinder;
	private resetButton: ElementFinder;
	private count: ElementFinder;

	constructor() {
		this.increaseButton = element(by.id('increase'));
		this.decreaseButton = element(by.id('decrease'));
		this.resetButton = element(by.id('reset'));
		this.count = element(by.id('count'));
	}

	public goToCounter(): Promise<void> {
		return browser.get('/#/counter');
	}

	increaseCount(): Promise<void> {
		return this.increaseButton.click();
	}

	decreaseCount(): Promise<void> {
		return this.decreaseButton.click();
	}

	resetCount(): Promise<void> {
		return this.resetButton.click();
	}

	public getCount(): Promise<string> {
		return this.count.getText();
	}

}