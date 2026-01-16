import { browser, ElementFinder, ElementArrayFinder, element, by } from 'protractor';

interface PersonalInformation {
  firstName: string;
  lastName: string;
  sex: string;
  experience: number;
  professions: string[];
  tools: string[];
  continent: string;
  commands: string[];
}

export class PersonalInformationPage {
  firstNameInput: ElementFinder;

  lastNameInput: ElementFinder;

  sexRadioInputs: ElementArrayFinder;

  experienceRadioInputs: ElementArrayFinder;

  professionCheckboxInputs: ElementArrayFinder;

  toolsCheckboxInputs: ElementArrayFinder;

  continentDropdownInput: ElementFinder;

  commandsListInput: ElementFinder;

  submitButton: ElementFinder;

  constructor() {
    this.firstNameInput = element(by.name('firstname'));
    this.lastNameInput = element(by.name('lastname'));
    this.sexRadioInputs = element.all(by.name('sex'));
    this.experienceRadioInputs = element.all(by.name('exp'));
    this.professionCheckboxInputs = element.all(by.name('profession'));
    this.toolsCheckboxInputs = element.all(by.name('tool'));
    this.continentDropdownInput = element(by.name('continents'));
    this.commandsListInput = element(by.name('selenium_commands'));
    this.submitButton = element(by.id('submit'));
  }

  public async goToWebsite(): Promise<void> {
    await browser.get('http://toolsqa.com/automation-practice-form/');
  }

  public async fillForm(personalInformation: PersonalInformation): Promise<void> {
    await this.fillFirstName(personalInformation.firstName);
    await this.fillLastName(personalInformation.lastName);
    await this.pickSex(personalInformation.sex);
    await this.pickExperience(personalInformation.experience);
    await this.checkProfessions(personalInformation.professions);
    await this.checkTools(personalInformation.tools);
    await this.pickContinent(personalInformation.continent);
    await this.pickCommands(personalInformation.commands);
    await this.submitPersonalInformation();
  }

  private async fillFirstName(firstName: string): Promise<void> {
    await this.firstNameInput.sendKeys(firstName);
  }

  private async fillLastName(lastName: string): Promise<void> {
    await this.lastNameInput.sendKeys(lastName);
  }

  private async pickSex(sex: string): Promise<void> {
    await this.sexRadioInputs.filter(element =>
      element.getAttribute('value').then(value => value === sex)
    ).first().click();
  }

  private async pickExperience(experience: number): Promise<void> {
    await this.experienceRadioInputs.filter(element =>
      element.getAttribute('value').then(value => value === `${experience}`)
    ).first().click();
  }

  private async checkProfessions(professions: string[]): Promise<void> {
    await this.professionCheckboxInputs.filter(element =>
      element.getAttribute('value').then(value => professions.some(e => e === value))
    ).click();
  }

  private async checkTools(tools: string[]): Promise<void> {
    await this.toolsCheckboxInputs.filter(element =>
      element.getAttribute('value').then(value => tools.some(tool => tool === value))
    ).click();
  }

  private async pickContinent(continent: string): Promise<void> {
    await this.continentDropdownInput.all(by.tagName('option')).filter(element =>
      element.getText().then(text => text === continent)
    ).first().click();
  }

  private async pickCommands(commands: string[]): Promise<void> {
    await this.commandsListInput.all(by.tagName('option')).filter(element =>
      element.getText().then(text => commands.some(command => text === command))
    ).click();
  }

  private async submitPersonalInformation(): Promise<void> {
    await this.submitButton.click();
  }
}