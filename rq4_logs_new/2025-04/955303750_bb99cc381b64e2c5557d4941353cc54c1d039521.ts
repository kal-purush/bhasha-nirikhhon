import { Page } from "@playwright/test";

export default class BaseElements {
    constructor(private page: Page) { }

    button(text: string): string {
        return `//button[normalize-space()="${text}"]`;
    }

    topBarMenuDropdown(menuName: string): string {
        return `//li[contains(@class,"topbar-body")]/span[normalize-space()="${menuName}"]`;
    }

    topBarMenuDropdownOption(option: string): string {
        return `//li[contains(@class,"topbar-body")]/ul//a[normalize-space()="${option}"]`;
    }

    sideBarMenuItem(itemName: string): string {
        return `//li[contains(@class,"main-menu-item")]//a[normalize-space()="${itemName}"]`;
    }

    pagesTitle(title: string): string {
        return `//header//div[contains(@class,"header-title")]//*[text()="${title}"]`;
    }

    inputFieldWithTitle(title: string): string {
        return `//label[normalize-space()="${title}"]/../following-sibling::div//*[contains(@class,"input")]`;
    }

    selectFieldWithTitle(title: string): string {
        return `//label[normalize-space()="${title}"]/../following-sibling::div//*[contains(@class,"select-text-input")]`;
    }

    selectFieldOption(value: string): string {
        return `//div[@role="option" and normalize-space()="${value}"]`;
    }

    checkBoxWithOptionAndTitle(title: string, option: string): string {
        return `//label[normalize-space()="${title}"]/../following-sibling::div//*[normalize-space()="${option}"]`;
    }
}